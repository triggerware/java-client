package nmg.softwareworks.jrpcagent;

import java.io.IOException;
import java.time.*;
import java.time.format.DateTimeParseException;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * <p>This class provides static methods for converting between the  Json Schema standard (ISO-8601)
 * representation of dates, times, and timestamps and corresponding values of classes in the 
 * <a href = "https://docs.oracle.com/en/java/javase/13/docs/api/java.base/java/time/package-summary.html"> java.time</a> package.
 * Triggerware client applications might need these for customized serializers/deserializers.  The default 
 * serialization/deserialization  of these java.time classes uses these methods, so application generally will not need
 * to use them explicitly.</p>
 * 
 * @author nmg
 *
 */
public class JsonTimeUtilities {
	/*private static String dateFormatPattern = "yyyy-MM-dd";
	private static DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern(dateFormatPattern, Locale.US);*/
	/**
	 * @param d the date to serialize
	 * @return the json schema serialization of the date
	 */
	public static String asJson(LocalDate d) {
		return d.toString();
		//return dateFormat.format(d);
		}

	/**
	 * @param jsdate  the json schema serial representation of a data
	 * @return The date represented by the json serialization, or null if the json is not recognized as a date.
	 */
	public static LocalDate dateFromJson(String jsdate) {
		try {
			return LocalDate.parse(jsdate);//LocalDate.parse(jdate, dateFormat);
		} catch (DateTimeParseException e) {
			return null;}
	}
	/*private static String timeFormatPattern = "HH:mm:ss 'Z' XXX",
						  timeFormatFractionalPattern = "HH:mm:ss.SSS 'Z' XXX";
	private static DateTimeFormatter timeFormat = DateTimeFormatter.ofPattern(timeFormatPattern, Locale.US),
				            timeFormatFractional = DateTimeFormatter.ofPattern(timeFormatFractionalPattern, Locale.US);*/

	/**
	 * @param t a time value to serialize
	 * @return the json schema serialization of the time
	 */
	public static String asJson(LocalTime t) {
		return t.toString();
		/*String encoding = (t.getNano() ==0) ? timeFormat.format(t) : timeFormatFractional.format(t);
		return encoding;*/
	}

	/**
	 * @param jst the json schema serialization of a time value
	 * @return the java time represented by the serialization, or null if the serialization is not recognized as a time.
	 */
	public static LocalTime timeFromJson(String jst) {
		return LocalTime.parse(jst);
		/*LocalDateTime d = null;
		try {
			d = LocalDateTime.parse(jt,timeFormatFractional);
		} catch (DateTimeParseException e) {
			try {
				d = LocalDateTime.parse(jt, timeFormat);
			} catch (DateTimeParseException e1) {	}
		}
		return (d==null)? null : d.toLocalTime();*/
	}

	/*private static DateTimeFormatter timestampFormat =  DateTimeFormatter.ofPattern(
			String.format("%s 'T' %s", dateFormatPattern, timeFormatPattern),  Locale.US);
	private static DateTimeFormatter timestampFormatFractional = DateTimeFormatter.ofPattern(
			String.format("%s 'T' %s", dateFormatPattern, timeFormatFractionalPattern), Locale.US);*/

	/**
	 * @param ts a java timestamp value (an Instant)
	 * @return the json schema serialization of the timestamp value
	 */
	public static String asJson(Instant ts) {
		/*String encoding = (ts.getNano() ==0) ? timestampFormat.format(ts)
											 : timestampFormatFractional.format(ts);*/
		return ts.toString();
	}

	/**
	 * @param jts a json schema serialization of a timestamp
	 * @return the java timestamp (an Instant) represented represented by the serialization, or null if the serialization 
	 * is not recognized as a timestamp.
	 */
	public static Instant timestampFromJson(String jts) {	
		//Instant ts = null;
		try {
			return Instant.parse(jts); // timestampFormatFractional);
		} catch (DateTimeParseException e) {
			/*try {
				ts = OffsetDateTime.parse(jts, timestampFormat);
			} catch (DateTimeParseException e1) {	}*/
		}
		return null;}
	

	/*
	 from 
	 https://www.nuwavesolutions.com/dynamically-parsing-json-to-arbitrary-java-classes-using-the-jackson-json-library/
	 */
	private final static SimpleModule isoModule = new SimpleModule();
	static {
		isoModule.addDeserializer(Instant.class, new JsonInstantDeserializer() );
		isoModule.addSerializer(Instant.class, new JsonInstantSerializer() );
	}
	public static void isoSerialization(JsonMapper om) {
		om.registerModule(isoModule);  //(new JavaTimeModule());}
		om.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
	}
	
	public interface CustomDeserializer{
	    Object parse(JsonParser parser) throws IOException;}
	
	private static final class JsonInstantDeserializer extends JsonDeserializer<Instant>{
		@Override
		public Instant deserialize(JsonParser parser, DeserializationContext arg1)
				throws IOException {
			var jts = parser.readValueAs(String.class);
			return timestampFromJson(jts);
		}
	}
	private static final class JsonInstantSerializer extends JsonSerializer<Instant>{
		@Override
		public void serialize(Instant instant, JsonGenerator gen, SerializerProvider provider)
				throws IOException {
			gen.writeString(asJson(instant));			
		}
	}
}
