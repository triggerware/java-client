package nmg.softwareworks.jrpcagent;

import java.util.concurrent.CompletableFuture;
import com.fasterxml.jackson.databind.JavaType;

/**
 * NamedParameterRequest is a template for a request that an agent may send to a JRPC server using the Json object form of params.
 * The method name for the request is specified in the template.
 * Optionally specify required and/or optional parameter names, allowing some validation of actual requests before they are transmitted to the TW server.
 * If the parameter names are not specified, or if the (inherited) field validateActualParameters is false (the default) then no client-side validation takes place.
 * This validation is limited to the names used for actual parameters, not to the values supplied for those parameters.
 * 
 * Subclass NamedParameterRequest if you want to override the inherited (no-op) implementation of establishResponseDeserializationAttributes
 * 
 * @param <T> the result type of the request
 */
public class NamedParameterRequest<T> extends OutboundRequest<T> {
	private final String[] optionalParameterNames, requiredParameterNames;
	
	/**
	 * @param resultType  the type of a successful result of the jrpc request
	 * @param meta
	 * @param method the string to use as the method key in the jrpc request
	 * @param requiredParameterNames
	 * @param optionalParameterNames
	 */
	public NamedParameterRequest(Class<T> resultType, /*Map<String, TreeNode> meta,*/ String method, String[] requiredParameterNames, String[] optionalParameterNames){
		super(resultType, /*meta,*/ method);
		this.optionalParameterNames = optionalParameterNames;
		this.requiredParameterNames = requiredParameterNames;
	}
	/*public NamedParameterRequest(TypeReference<T> resultTypeRef,  Map<String, TreeNode> meta, String method, String[] requiredParameterNames, String[] optionalParameterNames){
		super(resultTypeRef,  meta, method);
		this.optionalParameterNames = optionalParameterNames;
		this.requiredParameterNames = requiredParameterNames;
	}*/
	/**
	 * @param resultType  the type of a successful result of the request
	 * @param method the string to use as the method key in the jrpc request
	 */
	public NamedParameterRequest(JavaType resultType,  String method){
		this(resultType, /*null,*/ method, null, null);	}
	
	public NamedParameterRequest(JavaType resultType, /*Map<String, TreeNode> meta,*/ String method, String[] requiredParameterNames, String[] optionalParameterNames){
		super(resultType, /*meta,*/ method);
		this.optionalParameterNames = optionalParameterNames;
		this.requiredParameterNames = requiredParameterNames;
	}
	
	/**
	 * @return the optional parameter names for this request, if any were defined
	 */
	public String[] getOptionalParameterNames() {return optionalParameterNames;}
	/**
	 * @return the required parameter names for this request, if any were defined
	 */
	public String[] getRequiredParameterNames() {return requiredParameterNames;}
	void validate(NamedRequestParameters params) {
		params.validate(this, requiredParameterNames, optionalParameterNames);}

	/*public T execute(Connection c, NamedRequestParameters namedParameters) throws JRPCException {
		return execute(null, c, namedParameters);}*/

	public T execute(Connection c, NamedRequestParameters namedParameters) throws JRPCException {
		if (this.validateActualParameters) validate(namedParameters);
		//Object rt = /*(resultType == null) ? resultTyperef :*/ resultType;
		return c.synchronousRPC(this, /*rt, meta, methodName,*/ namedParameters);}

	public void notify (Connection c, NamedRequestParameters namedParameters) throws JRPCException {
		if (this.validateActualParameters) validate(namedParameters);
		c.notify(methodName, namedParameters);}
	
	/*public CompletableFuture<T> executeAsynch(Connection c,  NamedRequestParameters namedParameters) throws JRPCException {
		return executeAsynch(c,null,namedParameters);}*/

	public CompletableFuture<T> executeAsynch(Connection c,  NamedRequestParameters namedParameters) throws JRPCException {
		if (this.validateActualParameters) validate(namedParameters);
		@SuppressWarnings("unused")
		var rt = /*(resultType == null) ? resultTyperef :*/ resultType;
		return c.asynchronousRPC(this, /*rt,  resultInstance, meta, methodName,*/ namedParameters);}

	/*public void notifyAsynch(Connection c, NamedRequestParameters namedParameters) throws JRPCException {
		if (this.validateActualParameters) validate(namedParameters);
		c.asynchronousRPC(Void.class,  methodName, namedParameters);}*/
}
