package nmg.softwareworks.jrpcagent;

import java.io.IOException;
import java.util.Map;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import nmg.softwareworks.jrpcagent.JsonUtilities.JRPCGenerator;

/**
 * represents a single outgoing request or notification
 * @param <T> the type of response required for this request
 */
public class JRPCSimpleRequest<T> {
	interface SupplierWithException<T> {
		T get() throws IOException;
	}
	//protected final ObjectNode request;
	//private String preserialized = null;
	protected JRPCResponse<T> response = null;
	//two of the next three will be null
	protected final Class<T> resultClass;
	protected final TypeReference<T> resultTypeRef;
	protected final JavaType resultJType;
	protected final String methodName;
	protected final Object[] parameters;
	protected final NamedRequestParameters namedParameters;
	//protected final boolean serverAsynchronous;
	protected final Map<String,TreeNode> meta;
	protected final T instanceForResult;
	private final boolean isNotification;
	private final OutboundRequest<T> outbound;
	int requestId = -1; //for normal requests, this field is assigned in postRequest
	//public static boolean preSerializeRequests = false; //experimental, probably of no value

	/*public JRPCSimpleRequest(OutboundRequest<T> outbound, JRPCAgent agent, String methodName, T instanceForResult, 
			Object resultType, Object parameters) {
		this(outbound, agent, null, methodName, instanceForResult, resultType, parameters);}*/
	
	@SuppressWarnings("unchecked")
	JRPCSimpleRequest(OutboundRequest<T> outbound, JRPCAgent agent, Map<String, TreeNode> meta, String methodName, T instanceForResult, 
			Object resultType, Object parameters){
		this.meta = meta;
		this.outbound = outbound;
		if (resultType ==  null) {//a notification
			resultClass = null;
			resultJType = null;
			resultTypeRef = null;
			isNotification = true;
		} else if (resultType instanceof TypeReference) {
			resultClass = null;
			resultJType = null;
			resultTypeRef = (TypeReference<T>)resultType;
			isNotification = false;
		} else if (resultType instanceof JavaType) {
			resultClass = null;
			resultJType = (JavaType)resultType;
			resultTypeRef = null;
			isNotification = false;			
		} else {
			resultClass = (Class<T>) resultType;
			resultJType = null;
			resultTypeRef = null;
			isNotification = false;
		}
		this.methodName = methodName;
		if (parameters instanceof NamedRequestParameters) {
			this.parameters = null;
			this.namedParameters = (NamedRequestParameters) parameters;
		} else {
			this.parameters = (Object[]) parameters;
            this.namedParameters = null;
		}
		this.instanceForResult = instanceForResult;

		/*if (preSerializeRequests)
			this.request = (namedParameters!=null) ? bareRequestN(agent, methodName, namedParameters)
												: bareRequestP(agent, methodName, parameters);
		else this.request = null;
		if (request!=null)
			request.put("asynchronous", serverAsynchronous);*/
	}

	boolean isNotification() {return isNotification;}
	public String getMethodName() {return methodName;}
	public Object[] getParameters() {return parameters;}
	public Map<String,?> getNamedParameters() {return namedParameters;}
	public boolean isPositional() {return namedParameters==null;}
	public JRPCResponse<T> getResponse() {return response;}
	public Class<T> getResultClass(){return resultClass;}
	//public boolean isServerAsynchronous() {return serverAsynchronous;}
	boolean isCancelled() {return false;}

	int submit(Connection conn) throws IOException {
		//tw server log is in  /home/tw/logs/jsonrpc.log
		var agent = conn.getAgent();
		//int requestId = -1;
		if (!isNotification) {
			requestId = agent.nextRequestId();
			if (outbound != null) outbound.requestId = requestId;
			conn.addPendingRequest(requestId,this);
		}
		/*if (request != null) {
			var json = getRequestJsonText();
			request.put("id", requestId);
			synchronized(conn) {
				Logging.log("%s requesting: <%s>", agent.getName(), request);			
				conn.writeSocket(json);
			}
		} else {//stream request*/
			var text = streamRequest(conn, requestId);
			if (text != null) Logging.log("%s requesting: <%s>", agent.getName(), text);
		//}
		return requestId;
	}
	
	public void notify(Connection conn)throws IOException {
		var agent = conn.getAgent();
	    var text = streamNotification(conn);
		if (text != null) Logging.log("%s notifying: <%s>", agent.getName(), text);		
	}
	
	// write the portions of a message that are common to requests and notifications
	void streamRequestOrNotification(JRPCGenerator jrpcGen) throws IOException {
		jrpcGen.addStandardJRPCProperties( getMethodName());
		jrpcGen.streamMeta(meta);
		var jg = jrpcGen.getGenerator();
		//synchronized(jg) {
			jg.writeFieldName("params");
			if (isPositional()){
				jg.writeStartArray();
				for (var param : getParameters()) 
					jrpcGen.getMapper().writeValue(jg /*objMapper.getGenerator()*/, param);
				jg.writeEndArray();
			} else {
				jg.writeStartObject();
				for (var param : getNamedParameters().entrySet()) {
					jrpcGen.streamAttributeValue(param.getKey(), param.getValue());	}
				jg.writeEndObject();			
			}
		//}
	}
	private String streamNotification(Connection conn) throws IOException {
		JRPCGenerator jrpcGen = conn.getGenerator();
		var jg = jrpcGen.getGenerator();
		synchronized(jg) {
			jrpcGen.startLogging();
			//var jg = objMapper.getGenerator();
			jg.writeStartObject();
			streamRequestOrNotification(jrpcGen);
			jg.writeEndObject();
			jg.flush();
			return jrpcGen.logEntryComplete();
	 }		
	}

	private String streamRequest(Connection conn, int id) throws IOException {
		JRPCGenerator jrpcGen = conn.getGenerator();
		var jg = jrpcGen.getGenerator();
		synchronized(jg) {
			jrpcGen.startLogging();
			//var jg = objMapper.getGenerator();
			jg.writeStartObject();
			jg.writeNumberField("id", id);
			streamRequestOrNotification(jrpcGen);
			//jg.writeBooleanField("asynchronous", isServerAsynchronous());
			jg.writeEndObject();
			jg.flush();
			return jrpcGen.logEntryComplete();
	 }
	}

	public T handleSuccessResponse() {
		if (resultClass == Void.TYPE) return null;
		T javaValue = response.getResult();
		//if (javaValue != null) 
			return javaValue;
		/*else {var msg = String.format("RPC result not of expected type %s", resultClass);
			  Logging.log(msg);
			  throw new JRPCRuntimeException.DeserializationFailure(msg,null);
		}*/
	}

	public void completed(IncomingMessage /*Map<String, ?>*/ msg) {
		this.response = new JRPCResponse<T>(msg, this);
		this.notify(); //wake up thread, if any, waiting for responses
	}
	
	/*private static Object javaFromJson(Class<?>target, JsonNode jn) {
		if (target.isInstance(jn))	return jn;
		if (target == String.class && jn.isTextual()) return jn.asText();
		if ((jn.isValueNode()) != (target.isPrimitive())) 
			throw new JRPCRuntimeException(String.format("return type mismatch:expected %s received %s", target, jn));
		var jv = deserialize(jn,target);
		if (jv == null)
			throw new JRPCRuntimeException(String.format("unable to deserialize <%s> as an instance of %s",jn, target));
		else return jv;
	}*/
	
	/*public void preSerialize() {preserialized =  request.toString();}*/
	
	//private String getRequestJsonText() {	return preserialized == null ? request.toString() : preserialized;	}
	
	public Object deserializeResult(JsonParser jParser, Connection conn) {
		Object restoration = null;
		var agent = conn.getAgent();
		var mapper = conn.getObjectMapper();
		try {
			restoration = agent.prepareDeserializationState(mapper, meta);
			if (resultClass != null)
				return JRPCStreamParser.deserializeResult(jParser, resultClass, instanceForResult);
			if (resultTypeRef != null) return jParser.readValueAs(resultTypeRef);
			// parse with a JavaType target			
			return conn.getObjectMapper().readValue(jParser, resultJType);
			
		}catch(IOException e) {			
			throw new JRPCRuntimeException.DeserializationFailure("could not deserialize the result in a response",e);
		} finally {
			agent.restoreDeserializationState(mapper, restoration);
		}
	}

	/*private static final JsonNodeFactory jnfactory = JsonNodeFactory.instance;
	private static final MappingJsonFactory mjfactory = new MappingJsonFactory();
	private static ObjectMapper noStreamObjectMapper = new ObjectMapper(mjfactory);
	static {
		noStreamObjectMapper.registerModule(new JavaTimeModule());
		noStreamObjectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
	}*/
	/*private static ObjectNode bareRequestP(JRPCAgent agent, String methodName, Object ... parameters) {
	    var request = jnfactory.objectNode();
	    JsonUtilities.addStandardJRPCProperties(request, methodName);
	    request.put("id", agent.nextRequestId());
	    var paramObj = jnfactory.arrayNode();
	    for (var param : parameters) 
	    	paramObj.add(noStreamObjectMapper.valueToTree(param));
	    request.set("params", paramObj);
		return request;
	}
	private static ObjectNode bareRequestN(JRPCAgent agent, String methodName, NamedRequestParameters parameters) {
	    var request = jnfactory.objectNode();
	    JsonUtilities.addStandardJRPCProperties(request, methodName);
	    request.put("id", agent.nextRequestId());
	    //request.set("params", bserialize(parameters));
	    var paramObj = jnfactory.objectNode();
	    for (var pair:parameters.entrySet()) 
	    	paramObj.set(pair.getKey(), noStreamObjectMapper.valueToTree(pair.getValue()));
	    request.set("params", paramObj);
		return request;
	}*/
}
