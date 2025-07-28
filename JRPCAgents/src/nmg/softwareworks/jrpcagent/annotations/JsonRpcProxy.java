package nmg.softwareworks.jrpcagent.annotations;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface JsonRpcProxy{
	/**
	 * @return the JRPC request method name of the request this method submits.  If the return value is the empty string, which
	 * is the default, then the request method name is the same as the annotated method's own name.
	 */
    String methodName() default "";

	 /**
	 * @return true if this is a proxy for sending notifications rather than for requests.  Default is false.
	 */
     boolean isNotificationProxy() default false;

	 /**
	 * @return true if the automatic registration of this method should infer the params signature for jrpc requests from
	 * the parameter types of the annotated method.
	 */
     boolean signatureFromMethod() default true;

	 /**
	 * @return true if requests are sent using a Json Array for the params value, false if they are sent using a json Object.
	 */
     boolean positionalParameters() default true;

	 /**
	 * @return an  ordered array of property names that will be used in the Json Object params value in a request.
	 * The order of these names determines the correspondence of the values in that params value to the formal parameters
	 * of the annotated method.  If the value of parameter names is {} (the default) then the names used in a request must be
	 * same as the formal parameter names of the annotated method, and the order is the order of the formal paramters.
	 * Note -- by default the java compiler does NOT include formal parameter names in class files, and java reflection
	 * cannot recover those names.  If your annotated method is compiled this way, you MUST supply a non-default value for 
	 * parameterNames attribute in your annotation for proxies of requests with Json Object params values. 
	 * 
	 * The parameterNames attribute of the annotation is ignored if positionalParameters is true.
	 */
     String[] parameterNames() default {};  //ignored if positionalParameters is true
}