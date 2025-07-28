package nmg.softwareworks.jrpcagent;

import java.util.Map;

import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
//TODO would it be better to just have a JavaType, and not the special cases for a class or a typereference

/**
 * this is an abstract class extended by both {@link NamedParameterRequest} and {@link PositionalParameterRequest}
 * An instance of this class holds the method name and result type of a request that a JRPCAgent may send to its partner.
 * If the result type is Void, it may represent a notification.
 * The is a prototype of an actual request, in that it does not contain any parameter values.and has no field for a response
 * @author nmg	
 * @param <T> the result type of the request This must be Void for notifications
 */
abstract class OutboundRequest<T> {
	protected final String methodName;
	protected final Class<T>classz;
	protected final TypeReference<T>resultTyperef;
	protected final JavaType jResultType;
	//protected final boolean serverAsynchronous;
	protected final Map<String, TreeNode>meta;
	int requestId = -1;
	/**
	 * @param classz  the type that is the result of the request
	 * @param serverAsynchronous <code>true</code> if the request should be server-asynchronous, <code>false</code> otherwise.
	 * @param method the string that identifies this request to the server
	 */
	protected OutboundRequest(Class<T> classz, Map<String, TreeNode>meta, String method){
		this.classz = classz;
		this.resultTyperef = null;
		this.jResultType = null;
		this.methodName = method;
		this.meta = meta;
		//this.serverAsynchronous = serverAsynchronous;
	}
	/**
	 * @param resultTyperef the type that is the result of the request
	 * @param serverAsynchronous <code>true</code> if the request should be server-asynchronous, <code>false</code> otherwise.
	 * @param method the string that identifies this request to the server
	 */
	protected OutboundRequest(TypeReference<T> resultTyperef, Map<String, TreeNode>meta, String method){
		this.classz = null;
		this.resultTyperef = resultTyperef;
		this.jResultType = null;
		this.methodName = method;
		this.meta = meta;
		//this.serverAsynchronous = serverAsynchronous;
	}

	/**
	 * @param resultType the type that is the result of the request
	 * @param serverAsynchronous <code>true</code> if the request should be server-asynchronous, <code>false</code> otherwise.
	 * @param method the string that identifies this request to the server
	 */
	protected OutboundRequest(JavaType resultType,Map<String, TreeNode>meta, String method){
		this.classz = null;
		this.resultTyperef = null;
		this.jResultType = resultType;
		this.methodName = method;
		this.meta = meta;
		//this.serverAsynchronous = serverAsynchronous;
	}

	/**
	 * @return the name used by the server's Json rpc protocol for this method
	 */
	public String getMethod() {return methodName;}
	/**
	 * @return the Class that is the type of result returned by this ServerRequest
	 */
	public Class<T> getResultClass(){return classz;}
	
	/**
	 * @return the TypeReference that is the type of result returned by this ServerRequest
	 */
	public TypeReference<T> getResultTyperef(){return resultTyperef;}
	
	/**
	 * @return the JavaType that is the type of result returned by this ServerRequest
	 */
	public JavaType getJavaResultType(){return jResultType;}
	
	/**
	 * @return <code>true</code> if this request will be sent to the server for asynchronous excution,
	 *  otherwise <code>false</code>
	 */
	//public boolean isServerAsynchronous() {return serverAsynchronous;}
	
	public Map<String, TreeNode> getMeta(){return meta;}
	
	Object getResultType() {
		if (classz != null) return classz;
		if (resultTyperef != null) return resultTyperef;
		return jResultType;
	}
}

