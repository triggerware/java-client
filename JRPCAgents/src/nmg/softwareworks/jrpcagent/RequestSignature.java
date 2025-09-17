package nmg.softwareworks.jrpcagent;

import java.lang.reflect.*;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;


public abstract class RequestSignature {
	
		
	/*private static JavaType annotatedParameterType(Parameter p){
		var st = p.getAnnotation(SerializationType.class);
		if (st == null) return null;
		var gt = st.genericType();
		if (gt == null) gt = p.getType();
		return null;
	}*/

	static void logInterfaceWarning(Type t,  Method m) {
		if (!(t instanceof Class<?>)) return;
		var cls = (Class<?>)t;
		if (cls.isInterface())
		  Logging.log("using interface type <%s> for serialization in handler for %s", cls.getName(), m.getName());}

	/*public static RequestSignature signatureFromMethod(Method m, boolean positional, String[]parameterNames) {		
		var art = m.getAnnotatedReturnType();
		var rt = art.getType();
		//var st = art.getAnnotation(SerializationType.class);
		logInterfaceWarning(rt, m);
		if (positional) {
			var types = new Object[m.getParameterCount()-1];
			int i = 0;
			for (var param : m.getParameters()) {
				if (i>0) {
					var apt = param.getAnnotatedType();
					var pt = apt.getType();
					logInterfaceWarning(pt, m);
					types[i-1] = serializationTypeFor(pt);
				}
				i++;
			}		
			return new PositionalRequestSignature(types, serializationTypeFor(rt));
		} else {
			var paramType = new HashMap<String,Object>();
			var paramNames = new ArrayList<String>(m.getParameterCount()-1);
			int i = 0;
			for (var param : m.getParameters()) {
				if (i>0) { // the first parm must be the serverconnection
					var pname = parameterNames == null ? param.getName() : parameterNames[i-1];
					paramNames.add(pname);
					var apt = param.getAnnotatedType();
					var pt = apt.getType();
					logInterfaceWarning(pt, m);
					paramType.put(pname, serializationTypeFor(pt));
				}
				i++;
			}
			return new JsonObjectRequestSignature(paramType, serializationTypeFor(rt), paramNames);
		}	
	}*/
	protected final Class<?> resultClass;
	protected final TypeReference<?> resultTypeRef;
	protected final JavaType resultJType;
	protected final Class<?>[] exceptionTypes;
	
	public RequestSignature() {//for a notification
		resultClass = null;
		resultJType = null;
		resultTypeRef = null;
		exceptionTypes = null;		
	}
	public RequestSignature(Object resultType, Class<?>[] exceptionTypes){
		this.exceptionTypes = exceptionTypes;
		if (resultType instanceof TypeReference tr) {
			resultClass = null;
			resultJType = null;
			resultTypeRef = tr;
		} else if (resultType instanceof JavaType jt) {
			resultClass = null;
			resultJType = jt;
			resultTypeRef = null;			
		} else {
			assert resultType instanceof Class<?>;
			resultClass = (Class<?>) resultType;
			resultJType = null;
			resultTypeRef = null;
		}
	}
}
