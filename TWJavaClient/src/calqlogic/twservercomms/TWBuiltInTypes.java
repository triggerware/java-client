package calqlogic.twservercomms;

import java.io.IOException;
import java.math.BigInteger;
import java.time.*;
import java.util.Hashtable;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.node.*;

public class TWBuiltInTypes {


	private static Hashtable<String, Class<?>> columnClasses = new Hashtable<String, Class<?>>();
	static {
		/* for the old version of tw sql		 
		columnClasses.put("double", Double.TYPE);
		columnClasses.put("integer", Integer.TYPE);
		columnClasses.put("number", Number.class);
		columnClasses.put("boolean", Boolean.TYPE);
		columnClasses.put("stringcase", String.class);
		columnClasses.put("stringnocase", String.class);
		columnClasses.put("stringagnostic", String.class);
		 */
		columnClasses.put("tinyint", Byte.class);
		columnClasses.put("smallint", Short.class);
		columnClasses.put("integer", Integer.class); //not needed?
		columnClasses.put("int", Integer.class);
		columnClasses.put("bigint", Long.class);
		columnClasses.put("anyint", BigInteger.class);
		columnClasses.put("float", Float.class);
		columnClasses.put("double", Double.class);
		columnClasses.put("casesensitive", String.class);
		columnClasses.put("caseinsensitive", String.class);
		columnClasses.put("date", java.time.LocalDate.class);
		columnClasses.put("time", java.time.LocalTime.class);
		columnClasses.put("timestamp", java.time.Instant.class);
		columnClasses.put("interval", java.time.Duration.class);
		columnClasses.put("blob", DataBlob.class);
		columnClasses.put("json", TreeNode.class);
		columnClasses.put("", Object.class);
	}
	static Class<?> classFromName(String className){
		return columnClasses.get(className.toLowerCase());}
	
	static boolean isValidTypeName(String className) {
		return columnClasses.containsKey(className.toLowerCase());}
	
	private final static String sqlNullJsonSerialization = "*sqlnull*";
	
	
	private static Object parseTemporalValue(JsonParser jParser, Class<?> type, boolean validate) throws IOException {
		if (type == Duration.class) {
			var micros = jParser.getLongValue();
			var dur = Duration.ofNanos(micros *1000);
			return  dur == null ? null //sql null
					             :  dur;
		}
		if (type == Instant.class)
			return Instant.parse(jParser.getText());
		if (type == LocalDate.class)
			return LocalDate.parse(jParser.getText());
		if (type == LocalTime.class) {
			var micros = jParser.getLongValue();
			return LocalTime.ofNanoOfDay(micros *1000);
		}
		
		jParser.readValueAsTree(); //discard value
		throw new IOException(String.format("expected a <%s> serialization", type));
	}
	
	private static Number parseNumericValue(JsonParser jParser, Class<?> type, boolean validate) throws IOException {
		if (type == Object.class) return jParser.getNumberValue();
		var tkn = jParser.currentToken();
		if (type == Long.class || type == Integer.class || type == Short.class || type == Byte.class || type == BigInteger.class) {
			if (tkn == JsonToken.VALUE_NUMBER_INT) {
				if (type == Long.class) return jParser.getLongValue();
			    if (type == Integer.class) return jParser.getIntValue();
			    if (type == Short.class) return jParser.getShortValue();
			    if (type == Byte.class) return jParser.getByteValue();
			    if (type == BigInteger.class) return jParser.getBigIntegerValue();
			    if (type == Object.class) return jParser.getNumberValue();
			}
			jParser.readValueAsTree();
			throw new IOException(String.format("expected a <%s> serialization", type));
		}
		if (tkn != JsonToken.VALUE_NUMBER_FLOAT) {
			var tree = jParser.readValueAsTree();
			throw new IOException(String.format("expected a <%s> serialization", type));
		}
		if (type == Double.class) return jParser.getDoubleValue();
		if (type == Float.class) return jParser.getFloatValue();
		//should not be possible to reach this point

		jParser.readValueAsTree();
		throw new IOException(String.format("expected a <%s> serialization", type));
	}
	
	static Object parseOneValueRaw(JsonParser parser) throws IOException {//consumes value, does not advance to the next unconsumed token
		var tval = parser.readValueAsTree();
		if (tval instanceof ValueNode vn) {
			if (vn.isTextual()) return vn.asText();
			if (vn.isNull()) return null;
			if (vn instanceof NumericNode nn) return nn.numberValue();
		}
		throw new IOException("a column value was serialized as a non value node");
	}
	
	/**parse on TW SQL value using the parser, and leave the stream positioned at the next token following the value
	 * @param jParser the parser being used to deserialize the value
	 * @param type the java type expected for the value
	 * @param validate true if this function should not just assume that the serialization on the stream is suitable for the type expected. (not implemented)
	 * @return the jave value of the expected type
	 * @throws IOException
	 */
	static Object parseOneValue(JsonParser jParser, Class<?> type, boolean validate) throws IOException {//consumes one value from the current token, does not advance to the next unconsumed token
		//TODO: associate the code for parsing with the class object in a hash table
		if (type == Object.class)
			return parseOneValueRaw(jParser);
		var tkn = jParser.currentToken();
		if (tkn == JsonToken.VALUE_NULL) {
			jParser.nextToken(); //consume the null
			//when the target is JSON a null serialization reprsents json null
			//for all other types, it represent sql null, which is represented as Java null
			return (type == TreeNode.class) ? NullNode.getInstance() : null;
		}
		
		if (type.getName().startsWith("java.time."))  //a temporal class serialization
			return parseTemporalValue(jParser, type, validate);
		
		if (tkn.isNumeric()) 
			return parseNumericValue(jParser, type, validate);

		if (type == String.class) {
			if (tkn == JsonToken.VALUE_STRING) {
				return jParser.getText();		    
			} else {
				var tn = jParser.readValueAsTree();
				throw new IOException(String.format("expected a String serialization, received %s", tn));
			}
		}

		if (type == DataBlob.class) //TODO handle datablob
			return "";

		if (type == TreeNode.class) {
			var deserialized = jParser.readValueAsTree();
			return (deserialized instanceof TextNode && 
					((TextNode)deserialized).textValue().equals(sqlNullJsonSerialization)) ? null : deserialized;
		}
		
		return null;
	}

}
