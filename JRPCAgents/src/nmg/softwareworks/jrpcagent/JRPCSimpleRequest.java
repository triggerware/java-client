package nmg.softwareworks.jrpcagent;

//import static nmg.softwareworks.jrpcagent.JsonUtilities.jsonMapper;

import java.io.IOException;
import java.util.Map;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.json.JsonMapper;

/**
 * represents a single outgoing request or notification
 * @param <T> the type of response required for this request
 */
public class JRPCSimpleRequest<T> {
	interface SupplierWithException<T> {
		abstract T get() throws IOException;
	}
	//protected final ObjectNode request;
	//private String preserialized = null;
	protected JRPCResponse<T> response = null;
	//two of the next three will be null
	protected final Class<T> resultClass;
	//protected final TypeReference<T> resultTypeRef;
	protected final JavaType resultJType;
	protected final String methodName;
	protected final Object[] parameters;
	protected final NamedRequestParameters namedParameters;
	//meta contains non-standard attribute:value pairs to include in the request message
	//protected final Map<String,TreeNode> meta;
	//protected final T instanceForResult;
	private final boolean isNotification;
	private final OutboundRequest<T> outbound;
	int requestId = -1; //for normal requests, this field is assigned in postRequest

	/*public JRPCSimpleRequest(OutboundRequest<T> outbound, JRPCAgent agent, String methodName, T instanceForResult, 
			Object resultType, Object parameters) {
		this(outbound, agent, null, methodName, instanceForResult, resultType, parameters);}*/
	@SuppressWarnings("unchecked")
	private JRPCSimpleRequest(OutboundRequest<T> outbound, String methodName, Object resultType, Object parameters) {
		//this.meta = meta;
		this.outbound = outbound;
		this.methodName = methodName;
		if (resultType ==  null) {//a notification
			resultClass = null;
			resultJType = null;
			//resultTypeRef = null;
			isNotification = true;
		} /*else if (resultType instanceof TypeReference) {
			resultClass = null;
			resultJType = null;
			resultTypeRef = (TypeReference<T>)resultType;
			isNotification = false;
		} */ else if (resultType instanceof JavaType jrt) {
			resultClass = null;
			resultJType = jrt;
			//resultTypeRef = null;
			isNotification = false;			
		} else {
			resultClass = (Class<T>) resultType;
			resultJType = null;
			//resultTypeRef = null;
			isNotification = false;
		}
		if (parameters instanceof NamedRequestParameters nrp) {
			this.parameters = null;
			this.namedParameters = nrp;
		} else {
			this.parameters = (Object[]) parameters;;
			this.namedParameters = null;
		}
		//this.instanceForResult = null;//instanceForResult;

	}

	JRPCSimpleRequest(OutboundRequest<T> outbound, //JRPCAgent agent, Map<String, TreeNode> meta, String methodName,Object resultType, 
			  			Object parameters){
		this(outbound, outbound.methodName, outbound.getResultType(),  parameters);
	}
	JRPCSimpleRequest(boolean clientAsynchronous, //Map<String, TreeNode> meta,
			Object resultType,  String methodName, Object parameters){
		this(null, methodName, resultType, parameters);
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
	//OutboundRequest<T> getOutbound(){return outbound;}

	int submit(Connection conn) throws IOException {
		//tw server log is in  /home/tw/logs/jsonrpc.log
		var agent = conn.getAgent();
		//int requestId = -1;
		if (!isNotification) {
			requestId = agent.nextRequestId();
			//if (outbound != null) outbound.requestId = requestId;
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
			var jg = conn.getGenerator();
			synchronized(jg) {
				conn.startLogging(true);
				streamRequest(jg, conn.getPartnerMapper());//, requestId);
				jg.flush();
				var text = conn.getLoggedText(true);
				if (text != null) Logging.log("%s requesting: <%s>", agent.getName(), text);
				conn.afterWriteMessage(); jg.flush();
			}
		//}
		return requestId;
	}
	
	/*void notify(Connection conn)throws IOException {
	    var text = streamNotification(conn);
		if (text != null) Logging.log("%s notifying: <%s>", conn.getAgent().getName(), text);		
	}*/
	void streamAttributeValue(JsonGenerator toPartner, JsonMapper mapper, String attribute, Object value) throws IOException  {
		toPartner.writeFieldName(attribute);
		mapper.writeValue(toPartner, value);
	}


	/*private void streamMeta(JsonGenerator toPartner, JsonMapper mapper, Map<String, TreeNode> meta) throws IOException {
		if (meta!=null) {
			for (var pair: meta.entrySet())
				streamAttributeValue(toPartner, mapper, pair.getKey(),  pair.getValue());
		}
	}*/
	/* write the portions of a message that are common to requests and notifications
	   this code does not use signatures! It assumes that the java type of the value to be serialized
	   is sufficient to determine the serialization
	   This is only called from a context that is synchronized on the JsonGenerator writing the data
	 */
	void streamRequestOrNotification(JsonGenerator jg, JsonMapper mapper) throws IOException {
		JsonUtilities.addStandardJRPCProperties(jg, getMethodName());
		//streamMeta(jg, mapper, meta);
		jg.writeFieldName("params");
		if (isPositional()){
			jg.writeStartArray();
			for (var param : getParameters()) 
				mapper.writeValue(jg, param);
			jg.writeEndArray();
		} else {
			jg.writeStartObject();
			for (var param : getNamedParameters().entrySet()) {
				streamAttributeValue(jg, mapper, param.getKey(), param.getValue());	}
			jg.writeEndObject();			
		}
	}
	void streamNotification(JsonGenerator jg, JsonMapper mapper) throws IOException {
		jg.writeStartObject();
		streamRequestOrNotification(jg, mapper);
		jg.writeEndObject();
	}
	
	public void streamBatchRequestMember(JsonGenerator jg, JsonMapper mapper /*, int id*/) throws IOException {
		jg.writeStartObject();
		jg.writeNumberField("id", requestId);
		streamRequestOrNotification(jg, mapper);
		jg.writeEndObject();
	}

	private void streamRequest(JsonGenerator jg, JsonMapper mapper/*, int id*/) throws IOException {
		jg.writeStartObject();
		jg.writeNumberField("id", requestId);
		streamRequestOrNotification(jg, mapper);
		jg.writeEndObject();
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
	
	public Object deserializeResult(IncomingMessage response, JsonParser jParser, Connection conn) {
		//this is the orginal request
		//response is the message being deserialized.
		
		var dsstate = conn.getDeserializationState();
		//dsstate.put("request", outbound);
		try {
			if (resultClass != null)
				return conn.deserializeResult(jParser, resultClass);
			//if (resultTypeRef != null) return jParser.readValueAs(resultTypeRef);
			// parse with a JavaType target			
			else {
				//var xxx = jParser.readValueAsTree();
				return conn.getPartnerMapper().readValue(jParser, resultJType);
			}
			
		}catch(IOException e) {		
			var consumed = conn.getLoggedText(false);
			throw new JRPCRuntimeException.DeserializationFailure(
					String.format("could not deserialize the result in a response.<%s>", consumed), e);
		} 
	}

	void establishResponseDeserializationAttributes(IncomingMessage response, DeserializationContext deserializationContext) {
		if (outbound != null) outbound.establishResponseDeserializationAttributes(this, response, deserializationContext);	}
	
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
