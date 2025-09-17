package nmg.softwareworks.jrpcagent;

import java.lang.reflect.Method;
import java.util.*;
import com.fasterxml.jackson.databind.type.SimpleType;
import com.fasterxml.jackson.databind.type.TypeFactory;

public class JsonObjectRequestSignature extends RequestSignature {
	protected final Map<String, Object> jsonObjectParameterType; //each range element is either a Class, a TypeReference, or a JavaType
	private final List<String> paramNames;
	private final String[]ignoredParameterNames;
	Object[]defaultActualParameters;
	private Object[]getDefaultActuals(){
		Object [] defaultActuals = new Object[jsonObjectParameterType.size()]; //TODO: want a value other than null for primitive types
		for (var pair : jsonObjectParameterType.entrySet()) {
			var type = pair.getValue();
			if (isPrimitive(type)) {
				int index = parameterIndex(pair.getKey());
				var rc = ((SimpleType)type).getRawClass();
				defaultActuals[index] = defaultValueFor(rc);
			}
		}
		return defaultActuals;
	}
	
	public JsonObjectRequestSignature(Map<String, Object> paramsType,   List<String>paramNames){
		this(paramsType,Void.TYPE, null, paramNames);	}
	
	JsonObjectRequestSignature(Map<String, Object> paramsType, Object resultType,  Class<?>[] exceptionTypes, 
			List<String>paramNames){
		super(resultType, exceptionTypes);
		jsonObjectParameterType = paramsType;
		this.paramNames = paramNames;
		defaultActualParameters = getDefaultActuals();
		ignoredParameterNames = null;
		//TODO: validate the range elements of paramsType
	}
	
	JsonObjectRequestSignature (final Method m, String[]parameterNames){
		this(m, parameterNames, null, TypeFactory.defaultInstance());}

	JsonObjectRequestSignature (final Method m, String[]parameterNames, String[]ignoredParameterNames, TypeFactory tf) {
		super(tf.constructType(m.getReturnType()), m.getExceptionTypes());
		var paramsType = new HashMap<String,Object>();
		var paramNames = new ArrayList<String>(m.getParameterCount()-1); // no entry for the Connection param
		if (parameterNames!=null && parameterNames.length==0) parameterNames = null;
		int i = 0;
		for (var param : m.getParameters()) {
			if (i>0) { // the first parm must be the serverconnection
				var apt = param.getAnnotatedType(); 
				var pt = apt.getType();
				var pname = parameterNames == null ? param.getName() : parameterNames[i-1];
				paramNames.add(pname);
				logInterfaceWarning(pt, m);
				paramsType.put(pname, tf.constructType(pt));
			}
			i++;
		}
		this.jsonObjectParameterType = paramsType;
		this.paramNames = paramNames;
		this.ignoredParameterNames = ignoredParameterNames;
		defaultActualParameters = getDefaultActuals();
	}
	private static boolean isPrimitive(Object type) {
		return type instanceof SimpleType st && st.isPrimitive();}
	
	private static Object defaultValueFor(Class<?> type) {//TODO: fix this;
		if (type == int.class) return (int)0;
		if (type == long.class) return 0L;
		if (type == short.class) return 0;
		if (type == float.class) return 0.0f;
		if (type == double.class) return 0.0d;
		if (type == byte.class) return (byte)0;
		if (type == boolean.class) return false;
		if (type == char.class) return '\u0000';
		return null;
	}

	public  Map<String, Object> getParameterType(){return jsonObjectParameterType;}
	public Object[]getDefaultParameterValues(){return defaultActualParameters;}
	public int parameterIndex(String name) {
		return paramNames.indexOf(name);}
	
	boolean isIgnored(String attributeName) {
		return ignoredParameterNames!=null && 
				Arrays.stream(ignoredParameterNames).anyMatch(attributeName::equals);}
}
