package nmg.softwareworks.jrpcagent;
import static nmg.softwareworks.jrpcagent.JsonUtilities.*;


import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.*;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;


import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.*;
import com.fasterxml.jackson.databind.type.TypeFactory;

import nmg.softwareworks.jrpcagent.HandlerRegistration.RegisteredHandler;
import nmg.softwareworks.jrpcagent.JRPCException.JRPCClosedConnectionError;
import nmg.softwareworks.jrpcagent.annotations.JsonRPCException;
/**
 * <p>a Connection  represents a single connection between a pair of {@link JRPCAgent}s, referred to as 'partners'.
 * All communication between the agent and its partene takes place over one of these connections.
 * There may be multiple connection between the same pair of agents, and an agent may have connections to multiple partners.
 * </p><p>A Connection can be intentionally closed by the agent, after which it cannot be used for communication. 
 * There is no concept of reopening a connection.
 * </p><p>A Connection could also be closed by the partner.  This usually indicates a severe, and probably fatal, problem,
 * such as disagreement between the partners at the communication protocol level.
 * </p><p> Although a Connection encapsulates a  pair of streams, neither  the streams are exposed by the methods of this api.
 * </p><p>The communications that take place over a connection are</p>
 * <ul>
 * <li> requests issued by a JRPCAgent</li>
 * <li> responses to those requests issued by the agent's partner </li>
 * <li> notifications sent from one agent to its partner</li>
 * </ul>
 * <p>
 * For each open connection, the client contains a Java thread processing incoming data. This thread is responsible, among other things, 
 * for serializing requests and deserializing responses and notifications.  Errors encountered in serialization/deserialization
 * can cause the connection to be closed. This usually indicates a severe, and probably fatal, problem.  It could be a disagreement 
 * between the client and server about the protocol, or could be a bug in custom (de)serialization code supplied by the client 
 * (a bug in the client library itself is also a remote possibility).
 * </p>
 * <h2> Use of Multiple Connections from a single  client</h2>
 * <p> Use of multiple connections to a server from a single client is an advanced topic.  Multiple connections might be useful
 * (or even necessary) for acceptable performance of an application.  However, this need is only likely to arise when the application
 * is deployed at a sufficiently large scale (frequency of making requests), and is highly unlikely to arise during early development
 * of an application.
 * </p><p>For some applications, it will suffice to do all requests as synchronous calls in the client -- that is, like an ordinary function 
 * call, the result is returned as the value of the call. Since it involves a remote request the calling thread will block until the
 * result is available.  Such synchronous calls can  be carried out in multiple threads of the client -- the library code is thread
 * safe with respect to use of a connection, and multiple threads might be waiting on results of different requests simultanously.
 * 
 * </p><p>To determine whether there is a benefit to using multiple connections, you will need to consider the following aspects of the RPC 
 * implementation used by the Triggerware client.</p>
 * <ul>
 * <li> When a request is issued on connection C, its response will be delivered on connection C</li>
 * <li> When a subscription is registered on connection C, all notifications resulting from that subscription
 * will be de delivered on connection C</li>
 * <li> The deserialization of responses and notifications takes place in the thread of connection C. There is no
 * parallel deserialization of multiple responses/notifications.  The time it takes to deserialize a response or notification
 * is linear in the volume of data in that response or notification.</li>
 * <li> The serialization of a request on connection C takes place in the thread issuing the request, <i>not</i> in
 * C's thread.  If multiple threads serialize requests on a single connection C, this library ensures that the serializations
 * take place sequentially. The server will receive the requests sequentially.</li>
 * <li> Serialization of requests on connection C and deserialization of resonses/notifications on C <i>do not</i> require
 * sequentialization with respect to one another.  That is, issuing of requests on connection C from application threads 
 * does not block the activity of C's own thread.  They simply compete for processor time like any other threads.</li>
 * </ul>
 * <p>  The factors enumerated above are relevant to determining whether use of multiple connections from a single client
 * might allow greater paralellism in the client. The use of <i>asynchronous</i> request from a {@link JRPCAgent} can also reduce
 * blocking of agent threads, but this is simply a convenience feature for using Java
 * <a href = "https://docs.oracle.com/en/java/javase/13/docs/api/java.base/java/util/concurrent/CompletableFuture.html">
 *  CompletableFutures </a> with requests.
 * </p><p>
 * Contention for use of a connection, however, brings up an analogous situation in the server, which has its own potential for
 * a single connection being a bottleneck to parallelism for that specific client.  Every request serialized by a client must be 
 * deserialized by the server,
 * and every response/notification deserialized by the client's connection thread was previously serialized by the server.
 * Fortunately (for reasoning about parallelism, at least) the costs of serialization/deserialization in the server are also proportional
 * to the data volume, and the server is dealing with the <i>same</i> data values as the client, so there is no reason to 
 * use multiple connections to avoid a server bottleneck that is not also a reason to use them to avoid a client bottleneck.
 * </p><p>
 * In addition to the serialization/deserialization costs in the server,however, the server also must <i> carry out </i>
 * the request to produce a result. This cost is entirely request-dependent and may dwarf the serialization/deserialization
 * costs.  Since the server is unaware of threading architecture of the client, it allows the agent to control whether
 * requests received on a channel are carried out in parallel or sequentially.  The server has a single thread that is used
 * to compute results for <i>synchronous</i> requests on that channel, so these are handled sequentially by the server.
 * Each <i>asynchronous</i> request arriving on a channelis carried out in an ephemeral thread in the server, and thus does
 * not prevent the server from handling further requests on the same channel during the computation of the result.  The
 * {@link JRPCAgent} makes the decision whether to issue a given request as a synchronous or asynchronous request to the
 * server.
 * </p>
 */
public class Connection implements Closeable{
		
	private final OutputStream ostream;
	private final InputStream istream;
	private String name = null;
	private final SerializationState /*serializationState,*/ deserializationState;
	
	private static final int methodNotFoundCode = -32601;
	private static final int internalErrorCode = -32603; 

		private final JsonGenerator toPartner;
		private final JsonParser fromPartner;
		private boolean connected;
		
		public boolean busy = false;
		private final JRPCAgent agent;
		private final LoggingWriter lw;
		private final LoggingReader lr;
		private final JsonMapper partnerMapper; //a mapper configured for JRPC

		private final Thread.UncaughtExceptionHandler lastChance =  new Thread.UncaughtExceptionHandler(){
			@Override
			public void uncaughtException(Thread t, Throwable e) {
				Logging.log("uncaught exception of type %s in %s", e.getClass(), t);}			
		};
		private final Thread jrpcMessageHandler = new Thread() {
			public void run() {try {
				processMessagesFromPartner();} finally {try {
					istream.close();
				} catch (IOException e) {
				}}}
			@Override
			public Thread.UncaughtExceptionHandler getUncaughtExceptionHandler() {
				return lastChance;}
		};
		private final Thread jrpcNotificationHandler = new Thread() {
			public void run() {try {
				processNotificationsFromPartner();
			} catch (InterruptedException e) {//hopeless at this point.
			}}
		};
		private final Hashtable<Object,JRPCSimpleRequest<?>> pendingRequests = new Hashtable<>(); // key must be int or string

		class NotificationQueueEntry{
			private final Notification notification;
			private final String notificationMethod;
			NotificationQueueEntry(Notification notification, String notificationMethod){
				this.notification = notification;
				this.notificationMethod = notificationMethod;
			}
		}
		private final NotificationQueueEntry theFinalNotification = new NotificationQueueEntry(null,null);
		private final LinkedBlockingQueue<NotificationQueueEntry> pendingNotifications = new LinkedBlockingQueue<>(50);
		void enqueueNotification(Notification n, String notificationMethod) {
			try {pendingNotifications.put(new NotificationQueueEntry(n, notificationMethod));
			} catch (InterruptedException e) {}
		}
		
		protected Connection (JRPCAgent agent, InputStream istream, OutputStream ostream) throws IOException  {
			this.agent = agent;
			//serializationState = new SerializationState(this);
			deserializationState = new SerializationState(this);
			partnerMapper = jsonMapper(deserializationState);
			//each connection has its own mapper with its own serialization state, which allows serialization/deserializaton in parallel on different conncections
			connected = true;
			this.ostream = ostream;
			this.istream = istream;
			lw = new LoggingWriter(ostream);
			toPartner = JsonUtilities.createStreamSerializer(partnerMapper, lw);
			lr = new LoggingReader(istream);
			fromPartner = JsonUtilities.createStreamDeserializer(partnerMapper, lr);
			jrpcNotificationHandler.start();
			jrpcMessageHandler.start();
		}
		
		void startLogging(boolean out) {
			if (out)  lw.setLogging(true);
			else lr.setLogging(true);
		}
		
		String getLoggedText(boolean out) {
			var text =  (out) ? lw.getLoggedText(true) : lr.getLoggedText(true);
			return text.stripTrailing();
		}
		
		SerializationState getDeserializationState(){return deserializationState;}
		
		
		/**
		 * called after any complete JRPC message is written
		 * @throws IOException overrides may want to write
		 */
		public void afterWriteMessage() throws IOException {
			//lw.write("\n\n");
			}

		
		/**
		 * @return the JsonMapper for this connection's serialization and deserialization of 
		 * JSON requests/responses/notifications
		 */
		public JsonMapper getPartnerMapper() {return partnerMapper;}
		/**
		 * @return the JsonGenerator used to serialize this connection's JRPC messages
		 */
		public JsonGenerator getJsonGenerator () {return toPartner;}
		/**
		 * @return the JsonParser used to deserialize this connection's JRPC messages
		 */
		public JsonParser getJsonParser() {return fromPartner;}
		
		void handleUnexpected(IncomingMessage message, String fieldname) throws IOException {
			handleUnexpected(message,fieldname,false);}
		void handleUnexpected(IncomingMessage message, String fieldname, boolean unexpectedField) throws IOException {
			//stream has been advanced by nextToken()
			message.ensureUnexpected();
			var tnode = fromPartner.readValueAsTree();
			Logging.log("unexpected %s %s:<%s>", unexpectedField? "attribute" : "value", fieldname, tnode);
			message.unexpected.put(fieldname, tnode);			
		}

		
		//public JsonMapper getObjectMapper() {return partnerMapper;}

		/**
		 * @return this connection's input stream
		 */
		public InputStream getInputStream() {return istream;}

		/**
		 * @return this connection's output stream
		 */
		public OutputStream getOutputStream() {return ostream;}

		/**
		 * @return The JRPCAgent to which this connection belongs.
		 */
		public JRPCAgent getAgent() {return agent;}
		
		/**
		 * This is a convenience function.  
		 * @return the TypeFactory for this connection's ObjectMapper
		 */
		public TypeFactory getTypeFactory() {return partnerMapper.getTypeFactory();} 

		JsonGenerator getGenerator() {return toPartner;}
		
		JsonParser getParser() {return fromPartner;}
		
		/**
		 * Assign a name to this connection.  The name is used only in logging messages in this library.
		 * @param name The name to use for this connection.
		 */
		void setName(String name) {this.name = name;}
		
		public String getName() {
			return name == null ? agent.getName() : name;}

		/**
		 * onDisconnect is called when a connection is closed by the agent or by its partner.
		 * This default method does nothing, but subclasses may need to perform cleanup activities.
		 */
		protected void onDisconnect() {};
		
		private void notifyPending() {
			for (var pair : pendingRequests.entrySet()) {
				var request = pair.getValue();
				request.onConnectionClosed();
			}
			pendingRequests.clear();//unnecessary, but hygenic
		}

		private boolean disconnecting = false;
		private synchronized  void disconnect(){		
			if (!connected || disconnecting) return ;
			disconnecting = true;

			try {pendingNotifications.put(theFinalNotification); //causes the thread to terminate
			} catch (InterruptedException e1) {} 
			try	{
				istream.close();
				ostream.close();
				connected = false;
				if (!agent.isShuttingDown()) {
					notifyPending();
					onDisconnect();
				}
			}catch (IOException e){
				Logging.log(e,"Trouble disconnecting...");
			}
		}


		void writeWithSerializationState(JRPCAgent agent, Object value, Map<String,TreeNode>meta)  throws IOException {
			//var old = agent.prepareSerializationState(this, meta, value);
			IOException thrown = null;
			try {
				partnerMapper.writeValue(toPartner, value);
			}catch (IOException e) {thrown = e;
			} finally {
				//agent.restoreSerializationState(this, old, value);
				if (thrown != null) throw (thrown);}			
		}

		void streamResponseResultOrData(String attribute, Object value, Map<String,TreeNode>meta) throws IOException  {
			var isResult = attribute.equals("result");
			//var agent = mapper.getConnection().getAgent();
			toPartner.writeFieldName(attribute);
			writeWithSerializationState(agent, value, isResult ? meta : null);
		}

		private IncomingJRPCMessage next() throws IOException {
			// the stream may contain notifications, responses, and requests
			var tkn = fromPartner.nextToken();
		    if (tkn == null)
		    	throw new java.net.SocketException("null token indicates connection closed");
		    switch(tkn) {
		    	case START_OBJECT:
		    		return partnerMapper.readValue(fromPartner, IncomingMessage.class);
		    		
		    				
		    	case START_ARRAY:
		    		return IncomingBatch.parseOneBatchMessage(this);
		    	default:
					var tnode = fromPartner.readValueAsTree();
					Logging.log("unexpected message from server starting with token %s: <%s>", tkn, tnode);
					var message = new IncomingMessage(this);
					message.top = tnode;
					return message;
		    }
		}

		private void processMessagesFromPartner() {
			Logging.log("%s is processing jrpc messages", getName());
			try {
				//var rdr = new InputStreamReader(new DataInputStream(sock.getInputStream()), StandardCharsets.UTF_8);
				startLogging(false);
				while (true) {
					deserializationState.clear();
					var im = next();
					if (im == null) {
						Logging.log("partner closed the jrpc connection %s", getName());
						return; 
					}
					var imText = getLoggedText(false);
					if (!imText.isBlank()) 
						Logging.log("%s received <%s>", getName(), imText);
					if (im instanceof IncomingMessage m) {//not a batch message
						if (!m.isJrpcMessage()){
							var complaint = String.format("%s received Json<%s> from its partner where a JsonRPC message was required. Closing this connection",
									getName(), imText);
							throw new Exception(complaint);
						}
					}
					im.processMessage();
					
				}// end while true
			} catch (Throwable t) {
				if (disconnecting)
					Logging.log("clean shutdown of JRPC communications");
				else {
					var emsg = String.format("ignoring further input on connection %s", getName());
					Logging.log(t,emsg);
					disconnect();
				}
			}
		}
		
	private void processNotificationsFromPartner() throws InterruptedException {
		while (true) {
			var entry = pendingNotifications.take();
			if (entry == theFinalNotification) {
				Logging.log("notification handling is shutting down", getName());
				return; 
			}
			entry.notification.handle(this, entry.notificationMethod);
		}
	}

	JRPCSimpleRequest<?> pendingRequest(Object requestId) {
		return pendingRequests.get(requestId);	}
	
	void addPendingRequest(Object requestId, JRPCSimpleRequest<?> request) {
		pendingRequests.put(requestId, request);}
	
	void attachResponseToRequest(IncomingMessage msg) {
		//Utilities.log("Server response: [%s]", msg);
		var requestId = msg.id;
		var request = pendingRequests.remove(requestId);
		if (request == null) {
			Logging.log("response for %s but no outstanding request! Response ignored.", requestId);
			return;
		}
		//synchronized(request) {request.completed(msg);}
		request.completed(msg);
	}
	
	public void processRequestMessage(IncomingMessage msg) throws IOException {
		var rh = agent.getRequestHandler(msg.methodName);
		if (rh == null)
			streamErrorResponse(msg.id, methodNotFoundCode, "unregistered method name", msg.methodName, null);
		else if (msg.paramsDeserializingError != null) { //an error occurred processing the params
			var e = msg.paramsDeserializingError;
			streamErrorResponse(msg.id, e.getCode(), e.getLocalizedMessage(), e.getData(), null);
		} /*else if (msg.asynchronous) {
			executeRequest(msg, rh);//TODO implement thread pool handling of request.  Thread pool handler cannot throw an exception!
		}*/ else executeRequest(msg, rh);
	}
	private int getExceptionErrorCode(Exception e, RequestSignature sig) {
		if (sig.exceptionTypes != null) {
			for (var eklass : sig.exceptionTypes) {
				if (eklass.isInstance(e)) {
					var ea = eklass.getAnnotation(JsonRPCException.class);
					if (ea != null) return ea.errorCode();
				}
			}
		}
		return internalErrorCode;
	}
	private void executeRequest (IncomingMessage msg, RegisteredHandler rh) throws IOException {
		if (rh.synchronous) executeRequestInternal(msg,rh);
		/*else if (rh.useVirtualThread) {
			Thread thread = Thread.ofVirtual().start(() -> {// same as body of run below
			});
		}*/
		else {
			agent.executorService.execute(new Runnable() {//possible place to use virtual thread for java 21+
				@Override
				public void run() {
					try {
						executeRequestInternal(msg,rh);
					} catch (IOException e) {
						 try{
							 streamErrorResponse(msg, getExceptionErrorCode(e, agent.getRequestSignature(msg.methodName)), e,
								agent.errorResponseMetaProperties(msg.request, e));
						 }catch (Throwable t) {
							 Logging.log("fatal error on connection",t);
							 disconnect();
						 }
					}
				}
			});
		}
	}
	private void executeRequestInternal (IncomingMessage msg, RegisteredHandler rh) throws IOException {
		var sig = agent.getRequestSignature(msg.methodName);
		Object result = null;
		try {
			//per java documentation, if the method is STATIC, the first param passed to invoke is ignored!
			if (msg.positionalParams != null) {
			    msg.positionalParams[0] = this;
				result = rh.method.invoke(rh.instance,  msg.positionalParams);
			} /*else if (msg.jsonObjectParams != null) {
				//Logging.log("execution of server requests with named params not implemented");
				result = rh.invoke(this, msg.jsonObjectParams);
			}*/
		}catch (Exception e) {
			var cause =  (e instanceof InvocationTargetException ite) ? 
					(Exception)ite.getTargetException()
					: e;
			streamErrorResponse(msg, getExceptionErrorCode(cause, sig), cause, agent.errorResponseMetaProperties(msg.request, cause));
			return;
		}
		streamSuccessResponse(msg, result, null);		//TODO: how should an agent establish meta properties on a response?
	}
	
	void streamAttributeValue(String attribute, Object value) throws IOException  {
		toPartner.writeFieldName(attribute);
		partnerMapper.writeValue(toPartner, value);
	}

	private void streamMeta(Map<String, TreeNode> meta) throws IOException {
		if (meta!=null) {
			for (var pair: meta.entrySet())
				streamAttributeValue(pair.getKey(),  pair.getValue());
		}
	}
	private static Package javaLangPkg = Integer.TYPE.getPackage();
	private static NullNode jsonNull = NullNode.instance;
	private void streamResponseCommon(JsonGenerator jg, Object id, Map<String, TreeNode>responseMeta) throws IOException {
		jg.writeStringField("jsonrpc", "2.0");
		if (id instanceof Number)
			jg.writeNumberField("id", (int)id);
		else jg.writeStringField("id", (String)id);
		streamMeta(responseMeta);
	}
	
	//responseMeta is a set of propertyname/value pair to include at the top level
	//of the json response.  This is for properties NOT mentioned in the JRPC standard
	private void streamSuccessResponse(IncomingMessage msg, Object result, Map<String, TreeNode>responseMeta) throws IOException {
		var id = msg.id;
		var requestMeta = msg.getMetaProperties();
		synchronized(toPartner) {
			startLogging(true);
			var jg = toPartner;//.getGenerator();
			jg.writeStartObject();
			streamResponseCommon(jg, id, responseMeta);
			if (result == null)
				streamAttributeValue("result", jsonNull);
			else if (javaLangPkg == result.getClass().getPackage())
				streamAttributeValue("result", result);
			else streamResponseResultOrData("result", result, requestMeta);
			jg.writeEndObject();
			jg.flush();
			var sent = getLoggedText(true);
			Logging.log("%s sent  <%s>", getName(), sent);
		}
	}
	
	//responseMeta is a set of propertyname/value pair to include at the top level
	//of the json response.  This is for properties NOT mentioned in the JRPC standard
	private void streamErrorResponse(IncomingMessage msg, int errCode, Exception e, Map<String, TreeNode>responseMeta) throws IOException {
		//TODO: stream e itself as the data
		streamErrorResponse(msg.id, errCode, e.getLocalizedMessage(), msg.methodName, responseMeta);}
	private void streamErrorResponse(Object id, int errCode, String errMessage, Object data, Map<String, TreeNode> responseMeta) throws IOException {
		synchronized(toPartner) {
			startLogging(true);
			var jg = toPartner;//.getGenerator();
			jg.writeStartObject();
			streamResponseCommon(jg, id, responseMeta);
			jg.writeFieldName("error");
			jg.writeStartObject();
			streamAttributeValue("code",errCode);
			streamAttributeValue("message",errMessage);
			
			if (data == null){
			} else if (javaLangPkg == data.getClass().getPackage())
				streamAttributeValue("data", data);
			else streamResponseResultOrData("data", data, null);
			jg.writeEndObject();
			jg.writeEndObject();
			jg.flush();
			var sent = getLoggedText(true);
			Logging.log("%s sent  <%s>", getName(), sent);
		}
	}
	
	public Object deserializeParameter(JsonParser jParser, Class<?>paramsClass) throws IOException  {
		if (Void.class == paramsClass || Void.TYPE == paramsClass) {
			var params = jParser.readValueAsTree();
			//Logging.log("params for void method are %s",params);
			if (params instanceof ArrayNode an && an.size()==0)	return params;
			if (params instanceof ObjectNode on && on.size()==0)	return params;
			//Logging.log("wrong params for a void method [%s]", params);
			throw new JRPCRuntimeException.ActualParameterException("supplied parameters for a method with void parameters");
		}
		if (TreeNode.class.isAssignableFrom(paramsClass)){
				var parsed = jParser.readValueAsTree();
				if (paramsClass.isInstance(parsed))
					return parsed;
				else {	 //TODO: log an error
					return null;}
		} else 	return jParser.readValueAs(paramsClass);
	}
	
	Object deserializeResult(JsonParser jParser, Class<?>resultClass) throws IOException{
		if (//Void.class == resultClass ||
		Void.TYPE == resultClass) {
			return jParser.readValueAsTree(); // the result will be ignored  and is unconstrained. IT MUST NOT BE Java NULL
		}
		if (TreeNode.class.isAssignableFrom(resultClass)){
				var parsed = jParser.readValueAsTree();
				if (resultClass.isInstance(parsed))
					return parsed;
				else {
					Logging.log("request result type mismatch: expected %s received %s", resultClass, parsed.getClass());
					return null;
				}
		} else {
			return jParser.readValueAs(resultClass);
			/*if (instanceForResult == null) {
				//var jt = typeFactory.constructType(resultClass);
				return jParser.readValueAs(resultClass);}
			var jrpcr = (JRPCReader)jParser.getInputSource();
			return jrpcr.deserializeIntoObject(instanceForResult, jParser);*/
		}
	}

	public <T> T synchronousRPC(Object resultType, String method, Object params) throws  JRPCException {
		var jrpcRequest = new JRPCSimpleRequest<T>(false, resultType, method, params);
		return  synchronousRPC(jrpcRequest);
	}
	public <T> CompletableFuture<T> asynchronousRPC(Object resultType, String method,  Object ...params) throws  JRPCException {
		return asynchronousRPC(null, resultType, //meta,
							   method,  params);	}
	
	/**
	 * @param <T> the type of the result of this request
	 * @param req an OutboundRequest, the template for the request to issue
	 * @param params  either an Object[] for positional parameters or a NamedRequestParameters instance
	 * @return  the result value from the request
	 * @throws JRPCException if the response is an error response, or if some communication failure occurs
	 */
	public <T> T synchronousRPC(OutboundRequest<T> req,  Object params)	throws  JRPCException {
		var jrpcRequest = new JRPCSimpleRequest<T>(req, params);//createRequest(req, /*agent,*/ false, /*meta, resultType, method,*/ params);
		return  synchronousRPC(jrpcRequest);
	}

	/**
	 * @param <T> the type of the result of this request
	 * @param params  either an Object[] for positional parameters or a NamedRequestParameters instance
	 * @return  a CompletableFuture to deliver the result of the request
	 * @throws JRPCException  if some communication failure occurs delivering the request
	 */
	public <T> CompletableFuture<T> asynchronousRPC(OutboundRequest<T> req,  Object ...params) throws  JRPCException {
		var jrpcRequest = new JRPCAsyncRequest<T>(req, params);//(JRPCAsyncRequest<T>)createRequest(req, /*agent,*/ true, /*meta,resultType, req.methodName,*/ params);
		return asynchronousRPC(jrpcRequest);
	}
	
	public void notify(String method, Object params) throws JRPCClosedConnectionError{
		var jrpcRequest =  new JRPCSimpleRequest<Void>(false, null, method, params); //createRequest(null, agent, false,  /*null,*/ null, method, params);
		notify(jrpcRequest);
	}
	/*<T> T synchronousRPCP(TypeReference<T>tr, T resultInstance, boolean serverAsynchronous, String method, 
			Object ...params) throws  JRPCException {
		var jrpcRequest = createRequestP(this.getClient(), false, serverAsynchronous, tr, resultInstance, method, params);
		return synchronousRPC(jrpcRequest);
	}
	
	// use this overload for methods that want params as an object
	<T> T synchronousRPCN(Class<T>classz, T resultInstance, boolean serverAsynchronous, 
			String method, NamedRequestParameters params) throws JRPCException {
		var jrpcRequest = createRequestN(this.getClient(), false, serverAsynchronous, classz, resultInstance, method, params);
		return synchronousRPC(jrpcRequest);
	}*/
	
	private void notify(JRPCSimpleRequest<?> jrpcRequest) throws JRPCClosedConnectionError{
		synchronized(jrpcRequest) {	postRequest(jrpcRequest);}
	}
	
	/*private void asynchronousNotify(JRPCSimpleRequest<?> jrpcRequest) throws JRPCClosedConnectionError{
		var ab = agent.getActiveBatch();
		if (ab ==  null) synchronousNotify(jrpcRequest);
		else ab.addPendingRequest(this, jrpcRequest);
	}*/

	private <T> T synchronousRPC(JRPCSimpleRequest<T> jrpcRequest) throws  JRPCException {
		if (jrpcRequest.isNotification()) 
			throw new JRPCException.InternalJRPCException("internal error: synchronousRPC called on a notification");
		synchronized(jrpcRequest) {
			postRequest(jrpcRequest);
			try {jrpcRequest.wait();// will be awakended by request.notify
			} catch (InterruptedException e) {
				throw new JRPCException.InterruptionError(e);} 
		}
		var response = jrpcRequest.getResponse();
		if (response.hasResult())
		   return jrpcRequest.handleSuccessResponse();
		else {
			throw JRPCException.fromError(jrpcRequest,response.getError());
		}
	}

	public <T> CompletableFuture<T> asynchronousRPC(JRPCAsyncRequest<T> jrpcRequest) throws JRPCClosedConnectionError {
		var ab = agent.getActiveBatch();
		synchronized(jrpcRequest) {
			if (ab != null)
				ab.addPendingRequest(this, jrpcRequest);
			else
				postRequest(jrpcRequest);
			}
	    return jrpcRequest.getFuture();
	}

	/**
	 * writes a request onto the outbound stream of this connection.
	 * This method may be overridden to replace the serialization or to do something additional
	 * @param request the request to send
	 * @return the id assigned to the request
	 * @throws JRPCClosedConnectionError
	 */
	protected int postRequest(JRPCSimpleRequest<?> request) throws JRPCClosedConnectionError {
		if (isClosed())
			throw new JRPCException.JRPCClosedConnectionError(this);
		try{ 
		    return request.submit(this);
		}catch (IOException e)	{
			Logging.log(e, "in postRequest");
			throw new JRPCRuntimeException.SerializationFailure(
					String.format("failed to transmit request for [%s]",request.getMethodName()),
					e);
		}
	}
	
	void postBatchRequest(Collection<JRPCSimpleRequest<?>> requests) throws IOException {
		var jg = toPartner;
		synchronized(jg) {
			startLogging(true);
			for (var request : requests){
				jg.writeStartObject();
				request.streamBatchRequestMember(toPartner, partnerMapper);
				jg.writeEndObject();
				jg.flush();
			}
			jg.flush();
			var text =  getLoggedText(true);
			if (text != null) Logging.log("%s batch request: <%s>", getAgent().getName(), text);
		}		
	}

	void postNotification(JRPCSimpleRequest<?> notification) {
		try{ 
			synchronized (toPartner) {
				startLogging(true);
				notification.streamNotification(toPartner, partnerMapper);
			    toPartner.flush();
			    var text = getLoggedText(true);
				if (text != null) Logging.log("%s notifying: <%s>", getAgent().getName(), text);
			}
		}catch (IOException e)	{
			Logging.log(e, "in postNotification");
			throw new JRPCRuntimeException.SerializationFailure(
					String.format("failed to transmit notification for [%s]",notification.getMethodName()),
					e);
		}
	}

	/*@SuppressWarnings("unchecked")
	private <T> T handleResponse(JRPCRequest<T> request) throws JRPCException {
		var classz =  request.getResultClass();
		if (classz == Void.TYPE) return null;
		var response = request.getResponse();
		var jn = response.getResult();
		Object javaValue = javaFromJson(classz, jn);
		if (javaValue != null) return (T)javaValue;
		var msg = String.format("RPC result handling not yet implemented for result type %s", classz);
		Utilities.log(msg);
		throw new JRPCRuntimeException(msg);		
	}*/

	/*public void writeSocket (String text) throws IOException{
		sout.write(text);
		sout.flush();	
	}*/

	/**
	 * close this connection.  No more responses or notifications will be delivered from this connection.
	 */
	@Override
	public void close() {
		disconnect();
		//is there really any benefit to waiting for the threads to terminate here?
		try {
			jrpcMessageHandler.join(1000); 
		} catch (InterruptedException e) {
			// this would happen if something interrupted THIS thread while waiting for the join.
		}
		try {
			jrpcNotificationHandler.join(1000); 
		} catch (InterruptedException e) {
			// this would happen if something interrupted THIS thread while waiting for the join.
		}
	}

	public final boolean isClosed() {return !connected;}
}
