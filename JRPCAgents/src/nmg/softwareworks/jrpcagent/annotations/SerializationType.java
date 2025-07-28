package nmg.softwareworks.jrpcagent.annotations;

import java.lang.annotation.*;

/**
 * Add the SerializationType annotation to a parameter of a method having a JsonRpcHandler or JsonRpcProxy annotation
 * if the parameter type is generic or is an interface type.  This annotation affects the serialization/deserialization
 * of the parameter value.
 *
 *This annotation can also be placed on the method declaration itself if the return type is generic or is an interface type.
 */

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.PARAMETER, ElementType.TYPE_USE})
public @interface SerializationType {
	Class<?> genericType() ; 
	Class<?>[] typeParameters();
}
