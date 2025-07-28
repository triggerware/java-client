package nmg.softwareworks.jrpcagent;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;

/**
 * A request that uses positional parameters
 * @author nmg
 *
 * @param <T> the result type of the request
 */
public class PositionalParameterRequest<T> extends OutboundRequest<T> {
	private final Integer minParameterCount, maxParameterCount;
	public PositionalParameterRequest(Class<T> classz,  String method, Integer minParameterCount, Integer maxParameterCount){
		this(classz, null,method, minParameterCount, maxParameterCount);}
	public PositionalParameterRequest(Class<T> classz, Map<String, TreeNode> meta, String method, Integer minParameterCount, Integer maxParameterCount){
		super(classz, meta, method);
		this.minParameterCount = minParameterCount;
		this.maxParameterCount = maxParameterCount;
	}
	public PositionalParameterRequest(TypeReference<T> resultType, Map<String, TreeNode> meta, String method, Integer minParameterCount, Integer maxParameterCount){
		super(resultType, meta, method);
		this.minParameterCount = minParameterCount;
		this.maxParameterCount = maxParameterCount;
	}
	public PositionalParameterRequest(JavaType resultType, Map<String, TreeNode> meta, String method, Integer minParameterCount, Integer maxParameterCount){
		super(resultType, meta, method);
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
	public T execute(Connection c, Object ...positionalParameters) throws JRPCException{
		return execute(null,c,positionalParameters);}
	public T execute(T resultInstance, Connection c, Object ...positionalParameters) throws JRPCException {
		if (c.getAgent().validateParameters) validate(positionalParameters);
		return c.synchronousRPC(this,  resultInstance,positionalParameters);}

	public T mexecute(Connection c, Map<String, TreeNode>meta, Object ...positionalParameters) throws JRPCException{
		return mexecute(null,c,meta, positionalParameters);}
	public T mexecute(T resultInstance, Connection c, Map<String, TreeNode>meta, Object ...positionalParameters) throws JRPCException {
		if (c.getAgent().validateParameters) validate(positionalParameters);
		return c.synchronousRPC(this,  meta, positionalParameters);}
	
	public CompletableFuture<T> executeAsynch(Connection c, Object ...positionalParameters) 
			throws JRPCException{
		return executeAsynch(null, c, positionalParameters);}
	public CompletableFuture<T> executeAsynch(T resultInstance, Connection c, Object ...positionalParameters) 
			throws JRPCException{
		if (c.getAgent().validateParameters) validate(positionalParameters);
		return c.asynchronousRPC(this, getResultType(), resultInstance, Connection.serverAsynchronousMap, methodName, positionalParameters);}
	

	public void notify(Connection c, Object ...positionalParameters) throws JRPCException{
		if (c.getAgent().validateParameters) validate(positionalParameters);
		c.synchronousRPC(this, Void.class, null, null, methodName, positionalParameters);}

	public void notifyAsynch(Connection c, Object ...positionalParameters) 
			throws JRPCException{
		if (c.getAgent().validateParameters) validate(positionalParameters);
		c.asynchronousRPC(this, classz, null, null, methodName, positionalParameters);}
}

