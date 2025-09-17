package nmg.softwareworks.jrpcagent;

import java.lang.reflect.Method;

import com.fasterxml.jackson.databind.type.TypeFactory;

class PositionalRequestSignature extends RequestSignature {
	protected final Object[] positionalParameterTypes; // each element is either a Class, a TypeReference, or a JavaType
	PositionalRequestSignature(Object[] paramsType, Object resultType, Class<?>[]exceptionTypes){
		super(resultType, exceptionTypes);
		positionalParameterTypes = paramsType;
		//TODO: validate the elements
	}
	PositionalRequestSignature(Object[] paramsType){
		super();
		positionalParameterTypes = paramsType;
		//TODO: validate the elements
	}

	PositionalRequestSignature(Method m, String[]parameterNames){
		this(m, parameterNames, TypeFactory.defaultInstance());}

	PositionalRequestSignature(Method m, String[]parameterNames, TypeFactory tf){
		super(tf.constructType(m.getAnnotatedReturnType().getType()), m.getExceptionTypes());
		logInterfaceWarning(m.getReturnType(), m);
		var types = new Object[m.getParameterCount()-1];
		int i = 0;
		for (var param : m.getParameters()) {
			if (i>0) {
				var apt = param.getAnnotatedType();
				var pt = apt.getType();
				logInterfaceWarning(pt, m);
				types[i-1] = tf.constructType(pt);
			}
			i++;
		}
		positionalParameterTypes = types;
	}
	
	public Object[] getParameterTypes() {return positionalParameterTypes;}
}
