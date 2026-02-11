package nmg.softwareworks.jrpcagent;

//TODO: move more common code from ProxyRegistration and HandlerRegistration into this class

public final class HandlerProxyRegistration {
	public static final boolean publicOnlyReflection = true;
	/*static{
		var sm = System.getSecurityManager();
		if (sm == null) publicOnlyReflection = false;
		else try{
				sm.checkPermission(new java.lang.reflect.ReflectPermission(""));
				publicOnlyReflection = false;
			}catch(Exception e) {
			}
		}*/
}
