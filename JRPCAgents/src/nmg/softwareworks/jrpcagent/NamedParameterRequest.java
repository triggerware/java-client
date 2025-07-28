package nmg.softwareworks.jrpcagent;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;

/**
 * A server request that uses named parameters
 * @author nmg
 *
 * @param <T> the result type of the request
 */
public class NamedParameterRequest<T> extends OutboundRequest<T> {
	private final String[] optionalParameterNames, requiredParameterNames;
	private final HashSet<String>allParameterNames = new HashSet<String>();
	
	private void handleParamNames() {
		if (optionalParameterNames != null)
            Collections.addAll(allParameterNames, optionalParameterNames);
		if (requiredParameterNames != null)
            Collections.addAll(allParameterNames, requiredParameterNames);
	}

	
	public NamedParameterRequest(Class<T> classz, String method, String[] requiredParameterNames, String[] optionalParameterNames){
		this(classz, null, method, requiredParameterNames, optionalParameterNames);}
	public NamedParameterRequest(Class<T> classz, Map<String, TreeNode> meta, String method, String[] requiredParameterNames, String[] optionalParameterNames){
		super(classz, meta, method);
		this.optionalParameterNames = optionalParameterNames;
		this.requiredParameterNames = requiredParameterNames;
		handleParamNames();
	}
	public NamedParameterRequest(TypeReference<T> resultTypeRef, Map<String, TreeNode> meta, String method, String[] requiredParameterNames, String[] optionalParameterNames){
		super(resultTypeRef, meta, method);
		this.optionalParameterNames = optionalParameterNames;
		this.requiredParameterNames = requiredParameterNames;
		handleParamNames();
	}
	public NamedParameterRequest(JavaType resultType, Map<String, TreeNode> meta, String method, String[] requiredParameterNames, String[] optionalParameterNames){
		super(resultType, meta, method);
		this.optionalParameterNames = optionalParameterNames;
		this.requiredParameterNames = requiredParameterNames;
		handleParamNames();
	}
	
	public String[] getOptionalParameterNames() {return optionalParameterNames;}
	public String[] getRequiredParameterNames() {return requiredParameterNames;}
	void validate(NamedRequestParameters params) {
		params.validate(this, requiredParameterNames, optionalParameterNames);}

	public T execute(Connection c, NamedRequestParameters namedParameters) throws JRPCException {
		return execute(null, c, namedParameters);}
	public T execute(T resultInstance, Connection c, NamedRequestParameters namedParameters) throws JRPCException {
		if (c.getAgent().validateParameters) validate(namedParameters);
		Object rt = (classz == null) ? resultTyperef : classz;
		return c.synchronousRPC(this,rt, resultInstance, meta, methodName, namedParameters);}
	public void notify (Connection c, NamedRequestParameters namedParameters) throws JRPCException {
		if (c.getAgent().validateParameters) validate(namedParameters);
		c.synchronousRPC(Void.class, null, null, methodName, namedParameters);}
	
	public CompletableFuture<T> executeAsynch(Connection c,  NamedRequestParameters namedParameters) throws JRPCException {
		return executeAsynch(c,null,namedParameters);}
	public CompletableFuture<T> executeAsynch(Connection c, T resultInstance, NamedRequestParameters namedParameters) throws JRPCException {
		if (c.getAgent().validateParameters) validate(namedParameters);
		Object rt = (classz == null) ? resultTyperef : classz;
		return c.asynchronousRPC(this,rt, resultInstance, meta, methodName, namedParameters);}
	public void notifyAsynch(Connection c, NamedRequestParameters namedParameters) throws JRPCException {
		if (c.getAgent().validateParameters) validate(namedParameters);
		c.asynchronousRPC(Void.class, null, null, methodName, namedParameters);}
}
