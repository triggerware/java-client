package calqlogic.twservercomms;

import java.lang.annotation.*;

@Repeatable(ReusableQueries.class)
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface ReusableQuery {
	String query();
	String schema();
	String language() default Language.SQL;
	Class<?> rowType() default Object.class;
	String localName();
}
