package nmg.softwareworks.jrpcagent;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.databind.type.TypeFactory;

import nmg.softwareworks.jrpcagent.annotations.JsonRpcHandler;

/**
 * this class manages the registration of request handlers for JRPC agents
 *
 */
abstract public class HandlerRegistration extends Handler_Proxy_Registration {
	
	static class RegisteredHandler{
		final Method method;
		final Object instance;
		final boolean synchronous;
		//final String resultSerializationContext;
		RegisteredHandler(Method method, boolean synchronous, Object instance){
			this.method = method;
			this.instance = instance;
			this.synchronous = synchronous;
			//this.resultSerializationContext = resultSerializationContext;
		}
	}
	
    /**
     * This can be overridden to get a customized type factory. The factory is used to construct  JavaType instances
     * reflecting the types of request parameters/results and notifications.
     * 
     * @return a type factory
     */
    protected TypeFactory getTypeFactory() {
    	return TypeFactory.defaultInstance(); }
    
	//public static final boolean publicOnlyReflection = (System.getSecurityManager() == null);
	private static void validateHandlerMethod(Method m) throws Exception { //validations applicable to both request and notification handlers
		var paramCount = m.getParameterCount();
		if (paramCount==0)
			throw new Exception("A request or notification handler must have at least one parameter");
		var firstType = m.getParameterTypes()[0];
		if (!Connection.class.isAssignableFrom(firstType))
			throw new Exception("the first parameter of a request handling method must be ServerConnection or a subclass thereof");
		if (publicOnlyReflection && !Modifier.isPublic(m.getModifiers())) 
		   //private methods can be executed by reflection, but if there is a security manager they must be executed
		   //as a PrivilegedAction
			throw new Exception("A JRPC Agent request handler must be a public method when a Java Security Manager is used");
		//if (!(Modifier.isStatic(m.getModifiers()) || m.getDeclaringClass().isInterface() ||JRPCAgent.class.isAssignableFrom(m.getDeclaringClass())))
		//		throw new Exception("A JRPC Agent request or notification handler must be a static method or a method of a class that implements JRPCAgent");
		
	}

	private static void validateRequestHandlerMethod(Method m) throws Exception {
		validateHandlerMethod(m);}

	protected final Map<String, RequestSignature> requestSignatures = new HashMap<String, RequestSignature> ();
	public  Map<String, RequestSignature> getRequestSignatures() {return requestSignatures;}
	/*void clearRequestSignatures() {
		var sigs = getRequestSignatures();
		if (sigs != null) sigs.clear();}*/
	public RequestSignature registerRequestSignature(String methodName,  RequestSignature sig) {
		return getRequestSignatures().put(methodName, sig);}
	public RequestSignature registerRequestSignature(String methodName, Method m, boolean positional, String[]parameterNames,
			String[] ignoredParameterNames)	throws Exception {
		if (methodName.isBlank()) methodName = m.getName();
		RequestSignature sig =  positional ? new PositionalRequestSignature(m,  parameterNames, getTypeFactory())
										   : new JsonObjectRequestSignature(m,  parameterNames, ignoredParameterNames, getTypeFactory());
		return registerRequestSignature(methodName, sig);
	}
	public RequestSignature getRequestSignature(String method) { return getRequestSignatures().get(method);}

	protected Map<String, RegisteredHandler> requestHandlers = new HashMap<>();
	public  Map<String, RegisteredHandler> getRequestHandlers(){return requestHandlers;}
	//public void clearRequestHandlers() {getRequestHandlers().clear();}
	public  RegisteredHandler registerRequestHandler(String methodName, Method m, boolean synchronous, Object instance) throws Exception {
		validateRequestHandlerMethod(m);
		if (methodName.isBlank()) methodName = m.getName();
		if (Modifier.isStatic(m.getModifiers()))
			instance = null;
		else ; //TODO: verify that method is a method of instance
		m.setAccessible(true);
		var rh = new RegisteredHandler(m, synchronous, instance);
		return getRequestHandlers().put(methodName, rh);}
	public  RegisteredHandler getRequestHandler(String method) { return getRequestHandlers().get(method);}
	
	public  void registerRequestHandler(String methodName, RequestSignature sig, Method m, boolean synchronous,
			Object instance, String resultSerializationContext) throws Exception {
		if (methodName == null || methodName.isEmpty())
			throw new Exception(String.format("registerRequestHandler: invalid method name [%s]",methodName));
		registerRequestHandler(methodName, m, synchronous, instance);
		registerRequestSignature(methodName, sig);
	}
	

	/* notifications are now handled by notificationInducers
	 protected final Map<String, RequestSignature> notificationSignatures = new HashMap<String, RequestSignature> ();
	
	Map<String, RequestSignature> getNotificationSignatures() {return notificationSignatures;}
	public void clearNotificationSignatures() {
		var sigs = getNotificationSignatures();
		if (sigs != null) sigs.clear();}
	public RequestSignature registerNotificationSignature(String methodName,  RequestSignature sig) {
		return getNotificationSignatures().put(methodName, sig);}
	public RequestSignature registerNotificationSignature(String methodName, Method m, boolean positional, 
			String[]parameterNames)	{
		if (methodName.isBlank()) methodName = m.getName();
		//validateNotificationHandlerMethod(m);
		RequestSignature sig =  positional ? new PositionalRequestSignature(m,  parameterNames)
										   : new JsonObjectRequestSignature(m,  parameterNames);
		return registerNotificationSignature(methodName, sig);
	}*/
	
	protected final Map<String, NotificationInducer> notificationInducers = new HashMap<>();
	Map<String, NotificationInducer> getNotificationInducers(){
		return notificationInducers;}
	//public  void clearNotificationInducers() {notificationInducers.clear();}	
	public NotificationInducer getNotificationInducer(String method) { 
		return notificationInducers.get(method);}
	public Object registerNotificationInducer(String method, NotificationInducer ni)  {
		return notificationInducers.put(method, ni);}
	public  Object unregisterNotificationInducer(String method) {
		return notificationInducers.remove(method);}

	/**
	 * register all handlers specified by annotation in the class of this JRPCAgent or any of its superclasses.
	 */
	//public void registerHandlers() {registerHandlers(getClass());}
	
	public void registerHandlerFromAnnotation(JsonRpcHandler ann, Method method, Object instance) {
		try {
			var mName = ann.methodName();
			//var sc = ann.resultSerializationContext();
			registerRequestHandler(mName,  method, ann.synchronous(),  instance);
			var pnames = ann.parameterNames();
			if (pnames!=null && pnames.length==0) pnames = null;
			var positional = ann.positionalParameters() && pnames == null;
			if (ann.signatureFromMethod())
				registerRequestSignature(mName, method, positional, pnames, ann.ignoredParameterNames());
		} catch (Exception e) {
			Logging.log(e, "failed to register handler");
		}		
	}
	
	/**
	 * registerHandlers registers methods/signature for request and notification handlers of a class, its superclasses,
	 * and its interfaces.
	 * Registration is based on JsonRpcHandler annotations of methods declared in these classes and interfaces.
	 * @param klass The class for which request and notification handlers will be registered 
	 * 
	 * {@linkplain nmg.softwareworks.jrpcagent.annotations the annotations classes}
	 * 
	 * Note: this implementation currently does NOT register handlers if the annotation is placed on
	 * a DEFAULT method of an interface and none of the classes override the default!
	 */
	public <T>void registerHandlers(Class<T>klass) {registerHandlers(klass,null);}
	/**
	 * registerHandlers registers methods/signature for request handlers of a class, its superclasses,
	 * and its interfaces.
	 * Registration is based on JsonRpcHandler annotations of methods declared in these classes and interfaces.
	 * @param klass The class for which request and notification handlers will be registered 
	 * based on annotations of handler methods.
	 * @param instance an instance of klass. This is required if any of the annotated methods are non-static.  
	 * Those requests will be handled by the method of the instance supplied. It is unusual to have such handlers.
	 * 
	 * {@linkplain nmg.softwareworks.jrpcagent.annotations the annotations classes}
	 * 
	 * Note: this implementation currently does NOT register handlers if the annotation is placed on
	 * a DEFAULT method of an interface and none of the classes override the default!
	 */
	public <T>void registerHandlers(Class<T>klass, T instance) {
		var superClass = klass.getSuperclass();
		var interfaces = klass.getInterfaces();
		if (HandlerRegistration.class.isAssignableFrom(superClass)  && superClass != HandlerRegistration.class) 
			registerHandlers((Class<? super T>)superClass, instance);
		for (var method : klass.getDeclaredMethods()) {
			registerHandlersViaInterface(method, interfaces, instance);
			var jh = method.getAnnotation(JsonRpcHandler.class);
			if (jh != null) 
				registerHandlerFromAnnotation(jh, method, instance);
		}			
	}

	private void registerHandlersViaInterface(Method m, Class<?>[] interfaces, Object instance) {
	   String name=m.getName(); Class<?>[] paramTypes = m.getParameterTypes();
	   for(Class<?> i: interfaces)
	    try {
	        var ifM = i.getMethod(name, paramTypes);
	        var jh = ifM.getAnnotation(JsonRpcHandler.class);
	        if (jh != null)
	        	registerHandlerFromAnnotation(jh, m, instance);
	   } catch(NoSuchMethodException ex) {	   }
	}

}
