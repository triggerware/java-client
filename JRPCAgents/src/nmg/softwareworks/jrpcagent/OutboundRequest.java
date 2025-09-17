package nmg.softwareworks.jrpcagent;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;


/**
 * <p>
 * this is an abstract class extended by both {@link NamedParameterRequest} and {@link PositionalParameterRequest}
 * An instance of this class serves as a <em>template</em> for issuing actual requests.
 * The template holds the method name and the result type of a request that a JRPCAgent may send to its partner. * 
 * The template does not contain any parameter values.and has no field for a response, but may restrict the number of params(for array params requests)
 * or the key names that may appear (for object params requests)
 * </p><p>
 * A template for a request with no actual result should use Void.class for the result type. A template for a  notification should use null for the result type.
 * </p><p>
 * @param <T> the result type of the request This must be Void for notifications
 */
abstract class OutboundRequest<T> {
	protected final String methodName;
	protected final Class<T>resultType;
	protected final JavaType jResultType;
	//protected final TypeReference<T>resultTyperef;
	//protected final boolean serverAsynchronous;
	//protected final Map<String, TreeNode>meta;
	protected boolean validateActualParameters = false;
	/**
	 * @param classz  the type that is the result of the request
	 * @param meta extra key-value pairs to serialize in the request. The keys must not be standard jrpc keys.
	 * @param method the string that identifies this request to the server
	 */
	protected OutboundRequest(Class<T> resultType,  /*Map<String, TreeNode>meta,*/ String method){
		this.resultType = resultType;
		//this.resultTyperef = null;
		this.jResultType = null;
		this.methodName = method;
		//this.meta = meta;
		//this.serverAsynchronous = serverAsynchronous;
	}
	/**
	 * @param resultTyperef the type that is the result of the request
	 * @param meta extra key-value pairs to serialize in the request. The keys must not be standard jrpc keys.
	 * @param method the string that identifies this request to the server
	 */
	/*protected OutboundRequest(TypeReference<T> resultTyperef,  Map<String, TreeNode>meta, String method){
		this.resultType = null;
		this.resultTyperef = resultTyperef;
		this.jResultType = null;
		this.methodName = method;
		this.meta = meta;
		//this.serverAsynchronous = serverAsynchronous;
	}*/

	/**
	 * @param resultType the type that is the result of the request
	 * @param meta extra key-value pairs to serialize in the request. The keys must not be standard jrpc keys.
	 * @param method the string that identifies this request to the server
	 */
	protected OutboundRequest(JavaType resultType, /*Map<String, TreeNode>meta,*/ String method){
		this.resultType = null;
		//this.resultTyperef = null;
		this.jResultType = resultType;
		//this.deserializationAdvice = deserializationAdvice;
		this.methodName = method;
		//this.meta = meta;
		//this.serverAsynchronous = serverAsynchronous;
	}

	/**
	 * @return the name used by the server's Json rpc protocol for this method
	 */
	public String getMethod() {return methodName;}
	/**
	 * @return the Class that is the static type of result returned by this ServerRequest
	 */
	public Class<T> getResultClass(){return resultType;}
	
	//Object getdeserializationAdvice(){return deserializationAdvice;}
	
	/**
	 * Override this method on a subclass to establish context for deserialization of the result of a request
	 * @param response the message being deserialized as the response.  The already deserialized fields are available in the response message.
	 * @param attributes the attriubtes of the deserialization context being used to deserialize a response to this request.
	 */
	public void establishResponseDeserializationAttributes(JRPCSimpleRequest<?> request, IncomingMessage response, DeserializationContext context) {}
	
	/**
	 * @return the TypeReference that is the type of result returned by this ServerRequest
	 */
	//public TypeReference<T> getResultTyperef(){return resultTyperef;}
	
	/**
	 * @return the JavaType that is the type of result returned by this ServerRequest
	 */
	//public Object getResultType(){return resultType !=  null ? resultType : jResultType;}
	
	/**
	 * @return <code>true</code> if this request will be sent to the server for asynchronous excution,
	 *  otherwise <code>false</code>
	 */
	//public boolean isServerAsynchronous() {return serverAsynchronous;}
	
	//public Map<String, TreeNode> getMeta(){return meta;}
	
	Object getResultType() {
		if (resultType != null) return resultType;
		//if (resultTyperef != null) return resultTyperef;
		return jResultType;
	}
	public JRPCSimpleRequest<T> createRequest( JRPCAgent agent, boolean clientAsynchronous,
			/*Map<String, TreeNode> meta,*/ Object resultType, Object parameters) {
		return clientAsynchronous ? new JRPCAsyncRequest<T>(this, /*agent, meta, methodName,  resultType,*/  parameters) 
							      : new JRPCSimpleRequest<T>(this, /*agent, meta, methodName,  resultType,*/ parameters);
}
}

