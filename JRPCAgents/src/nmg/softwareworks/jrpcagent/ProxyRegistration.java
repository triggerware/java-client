package nmg.softwareworks.jrpcagent;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

import nmg.softwareworks.jrpcagent.annotations.JsonRpcProxy;
final class ProxyRegistration{
	
	
	private static void validateProxyMethod(Method m) throws Exception {
		/*var paramCount = m.getParameterCount();
		if (paramCount>1)
			throw new Exception("A JRPC Server request handler may have at most one parameter");*/
		if (HandlerProxyRegistration.publicOnlyReflection && !Modifier.isPublic(m.getModifiers())) 
		   //private methods can be executed by reflection, but if there is a security manager they must be executed
		   //as a PrivilegedAction
			throw new Exception("A JRPC request handler method must be a public method when a Java Security Manager is used");
		if (!(Modifier.isStatic(m.getModifiers())|| JRPCAgent.class.isAssignableFrom(m.getDeclaringClass())))
				throw new Exception("A JRPC request handler must be a static method or a method of a class that implements JRPCAgent");
	}
	
	private HashMap<String, RequestSignature> proxySignatures = new HashMap<>();
	public Map<String, RequestSignature> getProxySignatures(){return proxySignatures;}
	public void clearProxySignatures() {
		var sigs = getProxySignatures();
		if (sigs != null) sigs.clear();
	}
	public RequestSignature registerProxySignature(String methodName,  RequestSignature sig) {
		return getProxySignatures().put(methodName, sig);}
	public RequestSignature registerProxySignature(JRPCAgent agent, String methodName, Method m,  boolean positional, String[]parameterNames) 
			throws Exception {
		registerProxyMethod(methodName,m);
		RequestSignature sig = 
				positional ? new PositionalRequestSignature(m, parameterNames)
						   : new JsonObjectRequestSignature(m,  parameterNames);
		//RequestSignature.signatureFromMethod(m, positional, parameterNames);
		return registerProxySignature(methodName, sig);
	}
	public RequestSignature getProxySignature(String methodName) { return getProxySignatures().get(methodName);}
	
	private HashMap<String, Method> proxyMethods = new HashMap<>();
	public Map<String, Method> getProxyMethods(){return proxyMethods;}
	public void clearProxyMethods() {getProxyMethods().clear();}
	public Method registerProxyMethod(String methodName, Method m) throws Exception {
		if (methodName.isBlank()) methodName = m.getName();
		validateProxyMethod(m);
		return getProxyMethods().put(methodName, m);}
	
	public Method getProxyMethod(String methodName) { return getProxyMethods().get(methodName);}
	
	public void registerProxy(String methodName, RequestSignature sig, Method m) throws Exception {
		if (methodName == null || methodName.isEmpty())
			throw new Exception(String.format("registerProxy: invalid method name [%s]",methodName));
		registerProxyMethod(methodName, m);
		registerProxySignature(methodName, sig);
	}
	@SuppressWarnings("unchecked")
	public void registerProxies(JRPCAgent agent, Class<? extends JRPCAgent>klass) {
		var superClass = klass.getSuperclass();
		if (JRPCAgent.class.isAssignableFrom(superClass)) 
			registerProxies(agent, (Class<? extends JRPCAgent>)superClass);
		for (var method : klass.getDeclaredMethods()) {
			var jh = method.getAnnotation(JsonRpcProxy.class);
			if (jh == null) continue;
			//if (!checkAnnotation(method)) continue;
			try {
				registerProxyMethod(jh.methodName(),  method);
				var pnames = jh.parameterNames();
				if (jh.signatureFromMethod())
					registerProxySignature(agent, jh.methodName(), method, jh.positionalParameters(), pnames.length == 0? null : pnames);
			} catch (Exception e) {
				Logging.log(e, "failed to register request handler");
			} 
		}		
	}
}
