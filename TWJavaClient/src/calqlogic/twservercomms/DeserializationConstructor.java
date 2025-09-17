package calqlogic.twservercomms;

import java.lang.annotation.*;

/**
 * Add the DeserializationConstructor annotation to the constructor of a class used as the type for deserialization of a row of a ResultSet or Subscription notification
 */

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.CONSTRUCTOR})
public @interface DeserializationConstructor {
}
