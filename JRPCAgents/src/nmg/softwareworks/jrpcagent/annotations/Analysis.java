package nmg.softwareworks.jrpcagent.annotations;

import java.lang.reflect.*;
// I do not need this -- it seems that jackson's JavaType handles it (see typeFactory.constructType)
public abstract class Analysis {
	public static class ReflectionType{
		public final Class<?> rawClass;
		public final Type[] typeParameters;
		private ReflectionType(Class<?> rawClass, Type[] typeParameters) {
			this.rawClass = rawClass;
			this.typeParameters = typeParameters;
		}
	}
	public static ReflectionType genericBaseClass(Type tp){
		if (tp instanceof Class<?>) {
			if (((Class<?>)tp).getTypeParameters().length == 0)
				return new ReflectionType((Class<?>) tp, null);
		} else if (tp instanceof ParameterizedType) {
			Type[] actuals = ((ParameterizedType) tp).getActualTypeArguments();
			if (actuals.length>0) {
				var gtp = ((ParameterizedType) tp).getRawType();
				if (gtp instanceof Class<?>) return new ReflectionType((Class<?>) gtp, actuals);
			}
		}
		return null;
	}
}
