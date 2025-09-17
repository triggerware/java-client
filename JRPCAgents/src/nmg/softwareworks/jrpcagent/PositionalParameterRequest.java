package nmg.softwareworks.jrpcagent;

import java.util.concurrent.CompletableFuture;
import com.fasterxml.jackson.databind.JavaType;

/**
 * <p>PositionalParameterRequest is a template for a request that an agent may send to a JRPC server using the Json array form of params. 
 * The template contains the method name for the request.
 * The template may contain a minimum and/or maximum number of parameters, allowing some validation of actual requests before they are transmitted to the TW server.
 * If the parameter counts are not provided, or if the (inherited) field validateActualParameters is false (the default) then no client-side validation takes place.
 * This validation is limited to the number of actual parameters supplied, not to the values supplied for those parameters.
 * </p><p>
 * Subclass PositionalParameterRequest if you want to override the inherited (no-op) implementation of establishResponseDeserializationAttributes
 *</p>
 * @param <T> the result type of the request
 */
public class PositionalParameterRequest<T> extends OutboundRequest<T> {
	private final Integer minParameterCount, maxParameterCount;
	
	public PositionalParameterRequest(Class<T> resultType,  String method){
		this(resultType, /*null,*/ method, null, null);}

	public PositionalParameterRequest(Class<T> resultType, String method, Integer minParameterCount, Integer maxParameterCount){
		super(resultType, method);
		this.minParameterCount = minParameterCount;
		this.maxParameterCount = maxParameterCount;
	}
	/*public PositionalParameterRequest(TypeReference<T> resultType, Map<String, TreeNode> meta, String method, Integer minParameterCount, Integer maxParameterCount){
		super(resultType,  meta, method);
		this.minParameterCount = minParameterCount;
		this.maxParameterCount = maxParameterCount;
	}*/
	public PositionalParameterRequest(JavaType resultType, String method, Integer minParameterCount, Integer maxParameterCount){
		super(resultType, method);
		this.minParameterCount = minParameterCount;
		this.maxParameterCount = maxParameterCount;
	}
	void validate(Object ... params) {
		int n = params.length;
		if (minParameterCount != null && n<minParameterCount)
			throw new JRPCRuntimeException.ActualParameterException("too few parameters");

		if (maxParameterCount != null && n>maxParameterCount)
			throw new JRPCRuntimeException.ActualParameterException("too many parameters");
		
	}
	/*public T execute(Connection c, Object ...positionalParameters) throws JRPCException{
		return execute(null, c,positionalParameters);}*/

	public T execute(Connection c, Object ...positionalParameters) throws JRPCException {
		if (this.validateActualParameters) validate(positionalParameters);
		return c.synchronousRPC(this, positionalParameters);}

	/*public T mexecute(Connection c, Map<String, TreeNode>meta, Object ...positionalParameters) throws JRPCException{
		return mexecute(null,c,meta, positionalParameters);}

	public T mexecute(Connection c, Object ...positionalParameters) throws JRPCException {
		if (this.validateActualParameters) validate(positionalParameters);
		return c.synchronousRPC(this, positionalParameters);}*/
	
	public CompletableFuture<T> executeAsynch(Connection c, Object ...positionalParameters) 
			throws JRPCException{
		return executeAsynch(null, c, positionalParameters);}

	public CompletableFuture<T> executeAsynch(T resultInstance, Connection c, Object ...positionalParameters) 
			throws JRPCException{
		if (this.validateActualParameters) validate(positionalParameters);
		return c.asynchronousRPC(this, getResultType(), /* Connection.serverAsynchronousMap,*/ methodName, positionalParameters);}
	
	/*public void notify(Connection c, Object ...positionalParameters) throws JRPCException{
		if (this.validateActualParameters) validate(positionalParameters);
		c.synchronousRPC(this,  positionalParameters);}*/

	/*public void notifyAsynch(Connection c, Object ...positionalParameters) 
			throws JRPCException{
		if (this.validateActualParameters) validate(positionalParameters);
		c.asynchronousRPC(this, resultType, null, null, methodName, positionalParameters);}*/
}

