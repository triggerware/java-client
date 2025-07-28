package nmg.softwareworks.jrpcagent.annotations;

import java.lang.annotation.*;

/**
 * Place a JsonRPCException annotation on a subclass E of Exception to specify the integer error code
 * that should be used when a request handler throws E
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface JsonRPCException {
	/**
	 * @return the Json RPC error response code to use for this exception
	 */
    int errorCode();
}
