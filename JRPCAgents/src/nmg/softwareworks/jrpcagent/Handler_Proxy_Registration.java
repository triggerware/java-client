package nmg.softwareworks.jrpcagent;

//TODO: move more common code from ProxyRegistration and HandlerRegistration into this class

public class Handler_Proxy_Registration {
	protected static boolean publicOnlyReflection = true;
	static{
		var sm = System.getSecurityManager();
		if (sm == null) publicOnlyReflection = false;
		else try{
				sm.checkPermission(new java.lang.reflect.ReflectPermission(""));
				publicOnlyReflection = false;
			}catch(Exception e) {
			}
		}
}
