package nmg.softwareworks.jrpcagent.annotations;

import java.lang.annotation.*;

/**
 * Add the JsonRpcHandler annotation to a method of a JRPCServer that implements
 * handling of a specific request
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface JsonRpcHandler {

	/**
	 * @return the JRPC request method name of the request this method handles. If
	 *         the return value is the empty string, which is the default, then the
	 *         request method name is the same as the annotated method's own name.
	 */
    String methodName() default "";

	/**
	 * @return true if this is a handler for notifications rather than for requests.
	 *         Default is false.
	 */
    boolean isNotificationHander() default false;

	/**
	 * @return true if the automatic registration of this method should infer the
	 *         params signature for jrpc requests from the parameter types of the
	 *         annotated method.
	 */
    boolean signatureFromMethod() default true;

	/**
	 * @return true if requests are sent using a Json Array for the params value,
	 *         false if they are sent using a json Object. Note: if the
	 *         parameterNames property is non-empty, positionalParameters is ignored
	 *         and the parameters must be serialized as a Json Object
	 */
    boolean positionalParameters() default true;

	/**
	 * @return a string that will be used to establish context for serializing the
	 *         result. An empty string means no special context.
	 */
	// public String resultSerializationContext() default "";

	/**
	 * @return an ordered array of property names that will be used in the Json
	 *         Object params value in a request or notification. The order of these
	 *         names determines the correspondence of the values in that params
	 *         value to the formal parameters of the annotated method in the case
	 *         that positionalParameters is true. 
	 *         <p>If the value of parameter names is
	 *         {} (the default) then the names used in a request must be same as the
	 *         formal parameter names of the annotated method, and the order is the
	 *         order of the formal parameters. 
	 *         </p><p>
	 *         Note -- by default the java compiler
	 *         does NOT include formal parameter names in class files, and java
	 *         reflection cannot recover those names. If your annotated method is
	 *         compiled this way, you MUST supply a non-default value for
	 *         parameterNames attribute in your annotation for handling
	 *         requests/notifications with Json Object params values. 
	 *         </p><p>
	 *         If you compile your java file containing JsonRpcHandler declarations
	 *         with the <em>-parameters</em> compiler option the compiler will include 
	 *         formal parameter names in the class file and you can omit the parameterNames
	 *         argument in your annotation and have the desired defaulting behavior.
	 */
    String[] parameterNames() default {};
	
	/**
	 * @return a list of ignored parameter names (the ordered is irrelevant).
	 * Some apis have optional parameters,
	 * and the handler may choose to just ignore some of them (not even have formal paramter of the handler method
	 * to receive them)  Listing them here will prevent them (when the request supplies any of them) from
	 * generating log warnings.  The ignoredParameterNames and parameterNames must be disjoint.
	 */
    String[] ignoredParameterNames() default {};

	/**
	 * @return false if the handler should handle each request in a separate thread
	 */
    boolean synchronous() default true;
}
