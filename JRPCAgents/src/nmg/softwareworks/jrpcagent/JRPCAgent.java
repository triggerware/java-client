package nmg.softwareworks.jrpcagent;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

//import nmg.softwareworks.jrpcagent.JRPCObjectMapper;


/**
 *  <p>
 * A JRPCAgent represents a software agents that communicates with one or more partner agents using Json RPC requests, responses, 
 * and notifications. Each JRPCAgent holds a {@link Connection}s for each partner.  The connection is used by the agent to 
 * manage communications with its partner via a pair of streams.  
 *</p><p>
 *An agent's partner may be running in the same process or even in the same JVM as the agent.  
 *Or the partner may be running in a separate process, in which case that process may or may not be running on a different
 *host.  The partner may be a JRPC agent implemented with this library, but need not be implemented with this library or even 
 *with Java.  This is possible on a per-partner basis.
 *</p><p>
 *Communication management includes dispatching incoming <em>requests/notifications</em> to the appropriate handler registered with the JRPCAgent
 *and dispatching <em>requests</em> to the partner. 
 * JRPCAgent contains methods for sending requests (execute, executeAsynch) and notifications (notify)
 * to its partner. 
 * A request may be sent synchronously, in which case the sending thread blocks until the response is received.
 * A request may be sent asynchronously, in which case the caller receives a java CompletableFuture for dealing with
 * the response. There is no result of sending a notification. There is no confirmation that the notification has been handled
 * by the partner. 
 * </p><p>
 * Json RPC 'batch' requests are supported -- a batch requests may contain a mixture of asynchronous requests and notifications.
 * </p><p>
 *JRPCAgent contains methods for registering <em>handlers</em> for requests and notifications received by an agent.
 *The choice of handler is based on the value of the method property of the json object sent as the request or notification.
 *Handlers for requests and for notifications are registered independent of one another. It is allowed (but probably unwise) to
 *have a protocol in which some string may be used as the method of both requests and notifications.
 *</p><p>
 *Registration of handlers for requests and notifications is on a per-agent basis. (but see {@link JRPCServer}).
 *This means that if the agent has
 *registered  a handler for the method name "someMethod", that request will be dispatched to the handler regardless of which
 *of multiple partners sent the request.  The handler itself has access to the connection on which the request arrived, and so
 *may choose to respond differentially to requests from different partners.
 *</p><p>
 *The JRPCAgent class itself silently ignores notifications (other than logging).  Subclasses can deal with notifications by
 *registering a {@link NotificationInducer} for the method name that will be used in the notification.
 *</p><p>
 *An agent may be created by either supplying a pair of streams to give it an initial partner or by supplying a connected
 *Socket from which the stream pair may be found.
 *</p><p>
 *An agent may be released by executing its close method.  This closes its connection(s) with its partners.
 *</p>
 */
public abstract class JRPCAgent extends HandlerRegistration implements Closeable{
	
	final ExecutorService executorService = Executors.newFixedThreadPool(10);
	protected final InputStream istream;
	protected final OutputStream ostream;
	public InputStream getInputStream() {return istream;}
	public OutputStream getOutputStream() {return ostream;}
	private final Set<String>outboundMetaPropertyNames = new HashSet<String>(),
			                 inboundMetaPropertyNames = new HashSet<String>();
	static final String defaultName = "anonymous agent";
	private String name = defaultName;
	
	/**
	 * JRPCAgentException is the root class for exceptions that might be thrown by a JRPCAgent
	 * as a result of issuing a request or notification to its partner or handling a notification from its partner.
	 * @author nmg	 *
	 */
	public static class JRPCAgentException extends Exception{
		protected JRPCAgentException(String msg) {
			super(msg);	}
		protected JRPCAgentException(String msg, Throwable cause) {
			super(msg, cause);	}
	}
	
	/**
	 * This exception is signalled if a client attempts to issue a request on a connection
	 * that belongs to some other client.  Most requests will be issued through methods
	 * that do not specify a connection (implicitly using the 'primary' connection of the issuing client)
	 * @author nmg
	 *
	 */
	public static class InvalidConnectionException extends JRPCAgentException{
		private final Connection connection;
		public InvalidConnectionException(Connection connection) {
			super("agent using another agent's connection");
			//this.client = client;
			this.connection = connection;
		}

		/**
		 * @return the connection that was imroperly used.
		 */
		public Connection getConnection() {return connection;}
	}

	public Connection checkConnectionValid(Connection connection)throws InvalidConnectionException {
		if (connection == null) connection = primaryConnection;
		else {if (connection.getAgent() != this)	
				throw new InvalidConnectionException(connection);}
		if (connection.isClosed()) throw new InvalidConnectionException(connection);
		return connection;
	}
	
	private  AtomicInteger requestCounter = new AtomicInteger(1);
	protected int nextRequestId() {
		return requestCounter.getAndIncrement();	}

	

	/**
	 * An ObjectMapper will be determined by this method for each connection between this agent and a partner.
	 * getObjectMapper is invoked only when  a connection is established, and should NOT be called any other time.
	 * If the mapper holds mutable state (e.g., in its context attributes), then a new mapper should be allocated
	 * each time getObjectMapper is invoked.
	 * This method should be overridden in subclasses that need customized serialization/deserialization.
	 * @return the object mapper to use for serializing/deserializing over this agents connections
	 */
	/*public final JRPCObjectMapper getObjectMapper() {
		return defaultMapper;}*/
	/**
	 * Assign a name to this agent.  The name is used only in logging messages in this library.
	 * @param name The name to use for this agent.
	 */
	public void setName(String name) {this.name = name;}
	/**
	 * @return the name of this agent.
	 */
	public String getName() {return name;}
	
	/**
	 * Add one or more properties to allow on incoming JRPC messages in addition to those in the JRPC standard.
	 * @param propertyNames properties to all on incoming requests/notifications/responses
	 */
	public void addInboundProperties(String ...propertyNames) {
		for (var pname : propertyNames) inboundMetaPropertyNames.add(pname);	}
	/**
	 * Remove one or more properties to allow on incoming JRPC messages other than those in the JRPC standard.
	 * @param propertyNames properties to all on incoming requests/notifications/responses
	 */
	public void removeInboundProperties(String ...propertyNames) {
		for (var pname : propertyNames) inboundMetaPropertyNames.remove(pname);	}
	/**
	 * @param propertyName a possible property name
	 * @return true if propertyName is allowed on incoming JRPC messages
	 * Note: does not return true for the properties in the JRPC standard
	 */
	public boolean allowsInboundProperty(String propertyName) {
		return inboundMetaPropertyNames.contains(propertyName);}
	
	/**
	 * Add one or more properties to allow on outgoing JRPC messages in addition to those in the JRPC standard.
	 * @param propertyNames properties to allow on outbound requests/notifications/responses
	 */
	public void addOutboundProperties(String ...propertyNames) {
		for (var pname : propertyNames) outboundMetaPropertyNames.add(pname);	}
	/**
	 * Remove one or more properties to allow on outgoing JRPC messages other than those in the JRPC standard.
	 * @param propertyNames properties to all on outgoing requests/notifications/responses
	 */
	public void removeOutboundProperties(String ...propertyNames) {
		for (var pname : propertyNames) outboundMetaPropertyNames.remove(pname);	}
	/**
	 * @param propertyName a possible property name
	 * @return true if propertyName is allowed on outgoing JRPC messages
	 * Note: does not return true for the properties in the JRPC standard
	 */
	boolean allowsOutboundProperty(String propertyName) {
		return outboundMetaPropertyNames.contains(propertyName);}
	
	Map<String,TreeNode> errorResponseMetaProperties(JRPCSimpleRequest<?> request, Exception e){
		return null;}
	private final InetAddress inetAddr;
	private final Integer port;

	/**
	 * Create a new JRPCAgent using the two streams provided by a Socket.
	 * @param socket the socket providing the streams
	 * @throws IOException if a problem arises establishing the communications channels between the agents
	 */
	protected JRPCAgent(Socket socket, String name) throws IOException {
		istream = socket.getInputStream();
		ostream = new BufferedOutputStream(socket.getOutputStream());
		inetAddr = socket.getInetAddress();
		port = socket.getPort();
		if (!name.isBlank()) this.name = name;
		primaryConnection = connectToPartner(istream, ostream);
	}
	
	protected JRPCAgent(InputStream istream, OutputStream ostream, String name) throws IOException {
		this.istream = istream;
		this.ostream = ostream;
		inetAddr = null;
		port = null;
		if (!name.isBlank()) this.name = name;
		primaryConnection = connectToPartner(istream, ostream); 
	}
	
	protected final JsonMapper objectMapperForConnection(Connection c) {
		return c.getPartnerMapper();} 

	/**
	 * @return the InetAddress of this agents partner, if the partner is on a network connection.
	 */
	public final InetAddress getNetworkAddress() {return inetAddr;}
	/**
	 * This method creates a connection to a partner with which it may communicate using Json RPC
	 * An subclass of JRPCAgent may override this method to return a subclass of Connection if it wishes to implement
	 * differential behavior for distinct partners, including the possibility of maintaining per-partner state for the lifetime
	 * of the partnership.
	 * @param istream a stream that will read requests, responses, and notifications sent by the partner to this agent
	 * @param ostream a stream that will send requests, responses, and notifications to the partner from this agent
	 * @return a new Connection instance
	 * @throws IOException
	 */
	protected Connection createConnection(InputStream istream, OutputStream ostream) throws IOException {
		return new Connection(this, istream, ostream);	}
	/**
	 * This method creates a connection to a partner with which it may communicate using Json RPC
	 * This method uses createConnection to create the connection object
	 * @param istream a stream that will read requests, responses, and notifications sent by the partner to this agent
	 * @param ostream a stream that will send requests, responses, and notifications to the partner from this agent
	 * @return a new Connection instance
	 * @throws IOException
	 */
	public Connection connectToPartner(InputStream istream, OutputStream ostream) throws IOException {
		var c = createConnection(istream, ostream);
		addPartner(c);
		return c;
	}

	/**
	 * create an additional partner connection using the same remote server that was used to create this agent.
	 * @return a new Connection instance
	 * @throws IOException if this agent was not created supplying a Socket to its constructor, or
	 * it creating a new connection to the remote server fails.
	 */
	@SuppressWarnings("resource")
	public Connection connectToPartner() throws IOException {
		if (inetAddr == null) throw new IOException("this agent ws not created with a socket-based partner");
		var sock = new Socket(inetAddr, port);
		return connectToPartner(sock.getInputStream(), sock.getOutputStream());
	}

	static void validateNotificationType(Object jt) throws Exception {
		Class<?> baseClass = null;
		if (jt instanceof Class<?> c) baseClass = c;
		else if (jt instanceof JavaType jjt) baseClass = jjt.getRawClass();
		else if (jt instanceof TypeReference<?> tr) {
			//var om = new ObjectMapper( new MappingJsonFactory());
			baseClass = TypeFactory.defaultInstance().constructType(tr).getRawClass();
		}
		if (!Notification.class.isAssignableFrom(baseClass))
			throw new Exception(String.format("illegal notification type %s", jt));
	}

	/**
	 * a functional interface used for a computation that may perform a mixture of requests/notifications
	 * on one or more connections
	 *
	 */
	public interface BatchMessageComputation {
	    public void compute () throws Exception;
	}

	private static final ThreadLocal<Stack<BatchRequest>> activeBatch =
			new ThreadLocal<Stack<BatchRequest>>() {
		@Override 
		protected Stack<BatchRequest> initialValue() {
	        return new Stack<BatchRequest>();}
	    };
	BatchRequest getActiveBatch() {
		var q = activeBatch.get();
		return (q==null || q.isEmpty()) ? null : q.peek();
	}

	/**
	 * Send a batch request.
	 * @param comp a computation that sends asynchronous requests and/or notifications to this agent's partner. These asynchronous
	 * requests become the members of the batch request.  Synchronous requests and/or notifications may also be sent by the
	 * computation.  These operate just as they would <i>outside</i> of a batch computation.
	 * @throws Exception if the computation throws any exception
	 */
	public void batchRequest(BatchMessageComputation comp) throws Exception {
		var brs = activeBatch.get();
		if (brs == null) {
			brs = new Stack<BatchRequest>();
			activeBatch.set(brs);
		}
		var br = new BatchRequest();
		brs.push(br);
		var complete = false;
		try {
			comp.compute(); //uncaught exception aborts
			complete = true;
		}finally {
			brs.pop();
			if (brs.isEmpty()) //br was the outermost batch
				br.submit();
			else if (complete) {//successful execution of non-outermost batchRequest
				if (!br.isEmpty()) {
				  var newTop= brs.peek();
				  newTop.addAll(br);
				}
			}
		}		
	}
	
	protected boolean shuttingDown = false;
	public boolean isShuttingDown() {return shuttingDown;}

	/**
	 * <p>Closes all this agent's connections to its partners.  This agent can no longer be used
	 * to communicate with the partners. No further notifications will arrive from the partners.
	 * Outstanding requests will complete with the same exceptions that would have been used had the partner closed
	 * all the connections. No new connections may be established after shutdown has been invoked.
	 * </p><p>Individual connections may be shut down via the Connection's {@link Connection#close} method.
	 * </p>
	 */
	@Override
	public void close() {
		if (shuttingDown) return;
		shuttingDown = true;
		//jrpcNotificationsToHandle.add(theTerminatingNotification); // thread will terminate when it sees the null
		executorService.shutdown();
		//primaryConnection.close();
		allPartners.forEach((p)->p.close());
	}

	/**
	 * <p>An agent may have connections to any number of other agents.  Most agents, in most applications, will have just
	 * one connection. The first connection established for an agent is called its primary connection.
	 * </p><p>
	 * For convenience, many methods which need a connection object have a override that does not require the connection
	 * parameter, but instead use the primary connection. These overrides use the primary connection
	 * regardless of whether the agent has multiple connections, and regardless of whether the primary connection is still 
	 * open.</p>
	 */
	protected final Connection primaryConnection;
	/**
	 * @return the primary connection of this agent
	 */
	public Connection getPrimaryConnection() {return primaryConnection;}
	
	//public TypeFactory getTypeFactory() {return primaryConnection.getTypeFactory();}

	public <T> T execute(NamedParameterRequest<T> request, NamedRequestParameters namedParameters) throws JRPCException {
		return request.execute(primaryConnection, namedParameters);}

	public <T> T execute(PositionalParameterRequest<T> request, Object...parameters ) throws JRPCException {
		return request.execute(primaryConnection, parameters);}	

	/*public <T> T mexecute(PositionalParameterRequest<T> request, Map<String, TreeNode>meta,  Object...parameters ) throws JRPCException {
		return request.mexecute(primaryConnection, meta, parameters);}*/
	
	public <T> CompletableFuture<T> executeAsynch(NamedParameterRequest<T> request, NamedRequestParameters namedParameters) throws JRPCException {
		return request.executeAsynch(primaryConnection, namedParameters);}

	public <T> CompletableFuture<T> executeAsynch(PositionalParameterRequest<T> request, Object...parameters ) throws JRPCException {
		return request.executeAsynch(primaryConnection, parameters);}
	
	public void notify(NamedParameterRequest<Void> request, NamedRequestParameters namedParameters) throws JRPCException {
		primaryConnection.notify(request.methodName, namedParameters);}

	public void notify(PositionalParameterRequest<Void> request, Object...parameters ) throws JRPCException {
		primaryConnection.notify(request.methodName, parameters);}
	
	/*public void notifyAsynch(NamedParameterRequest<Void> request, NamedRequestParameters namedParameters) throws JRPCException {
		request.notifyAsynch(primaryConnection, namedParameters);}

	public void notifyAsynch(PositionalParameterRequest<Void> request, Object...parameters ) throws JRPCException {
		request.notifyAsynch(primaryConnection, parameters);}*/

	//boolean validateParameters = false;
	
	/**
	 * @return <code>true</code> if this JRPCAgent validates parameters prior
	 * to issuing a request, otherwise <code>false</code>  Default is <code>false</code>
	 * For positional parameter requests, validation checks the the number of supplied parameters is within
	 * the allowed range.  For named parameter requests, validation ensures the all required parameters are supplied
	 * and that all supplied parameters are either required or optional.
	 */
	//public boolean validatesParameters() {return validateParameters;}
	
	/**
	 * @param validate <code>true</code> to make this JRPCAgent validate parameters prior
	 * to issuing a request, <code>false</code> to leave validation to the server.
	 * @return the previous parameter validation setting.
	 */
	/*public boolean validateParameters(boolean validate) {
		var result = validateParameters;
		validateParameters = validate;
		return result;
	}*/
	
	protected final Set<Connection> allPartners = 
			Collections.synchronizedSet(new HashSet<Connection>());
	private void addPartner(Connection c) {allPartners.add(c);}
	
	/**
	 * called at the start of serialization of a result or error response
	 * @param mapper the JRPCObjectMapper being used for serialization
	 * @param meta configuration data to control the serialization of value
	 * @param value The value to be serialized
	 * @return the value to be passed to restoreSerializationState when the serialization is complete
	 */
	/*protected JRPCSerializationState prepareSerializationState(Connection c,   Map<String,TreeNode> meta, Object value) {
		var iss = c.getSerializationState();
		return (iss == null)? null : iss.setState(meta,  true);}*/
	
	/**
	 * called when the serialization of a result or error response completes
	 * @param Connection the Connection on which serialization is occurring
	 * @param restoration the value to use for restoring the serialization state (returned by updateSerializationState)
	 */
	//protected void restoreSerializationState(Connection c, Object restoration) {
	//	c.getSerializationState().restoreStateForGenerating(restoration);}
	
	/**
	 * called at the start of a top-level deserialization
	 * @param mapper the JRPCObjectMapper being used for deserialization
	 * @param request If this top level deserialization is for the response of a request, this is that request.  If not, this is null.
	 * @return the value to be passed to restoreSerializationState when the deserialization is complete
	 */
	/*final Object prepareDeserializationState(Connection c, Map<String,TreeNode> meta) {
		var ids = c.getDeserializationState();
		return (ids == null)? null : ids.setState(meta, true);
	}*/
	
	/**
	 * called when a top-level deserialization completes
	 * @param mapper the JRPCObjectMapper being used for serialization
	 * @param restoration the value to use for restoring the deserialization state (returned by setSerializationState)
	 */
	//public void restoreDeserializationState(Connection c, Object restoration) {
	//	c.getSerializationState().restoreStateForParsing(restoration);}


	/*public static Class<?> genericBaseClass(java.lang.reflect.Type tp){
		if (tp instanceof Class<?>) {
			if (((Class<?>)tp).getTypeParameters().length > 0)
				return (Class<?>) tp;
		} else if (tp instanceof java.lang.reflect.ParameterizedType) {
			if ((((java.lang.reflect.ParameterizedType) tp).getActualTypeArguments().length>0)) {
				var gtp = ((java.lang.reflect.ParameterizedType) tp).getRawType();
				if (gtp instanceof Class<?>) return (Class<?>)gtp;
			}
		}
		return null;
	}*/
	
	 //public  java.util.function.Consumer<Integer> foo() {return null;}
	 /*public static class Foo {
		 @JsonProperty("s")
		 Short s;
		 Foo(Short s){this.s = s;}
		 public Foo() {} //for deserializing
		 
	 }*/
	 /*public static void main(String[] args) throws Exception {
		 var mapper = JsonUtilities.jsonMapper();
		 var parser = mapper.createParser(" { \"s\" : null }  ");
		 var foo = parser.readValueAs(Foo.class);
		 var ostream = new ByteArrayOutputStream();
		 var generator = mapper.createGenerator(ostream);
		 mapper.writeValue(generator, foo);
		 var generated = new String(ostream.toByteArray());
		 
		 generated = generated;
		var m = JRPCAgent.class.getMethod("foo");
		var art = m.getAnnotatedReturnType();
		var tf = TypeFactory.defaultInstance();
		var jt = tf.constructType(art.getType());
		var gbc = 	nmg.softwareworks.jrpcagent.annotations.Analysis.genericBaseClass(art.getType());

		var st = art.getAnnotation(nmg.softwareworks.jrpcagent.annotations.SerializationType.class);
		st = st;
	}*/
	/*public static void main(String[] args) {
		//var xxx = activeBatch.get();
		var future = new CompletableFuture<Integer>();
		try {future.get(3,TimeUnit.SECONDS);
		
		}catch(java.util.concurrent.TimeoutException to) {
			to = new java.util.concurrent.TimeoutException("client timeout  waiting for");
			to = to;
		}catch (Throwable t) {
			t = t;}
	}*/
}
