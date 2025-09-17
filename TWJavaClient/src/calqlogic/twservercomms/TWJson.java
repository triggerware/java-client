package calqlogic.twservercomms;

import java.io.IOException;
import java.io.OutputStream;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;

/**
 * TWJson is a wrapper around a TreeNode. It is used as the Java representation of a value of the TW type Json
 * That abstract type includes all Json values as well as the sql null value.
 * For this type, the sql null value is serialized with a reserved Json string "*sqlnull*", and so the TW type Json is
 * actually missing that one value from the abstract type. 
 * 
 * Because jackson does not hand null values to custom serializers, there is no custom serializer for TWJson.
 * Instead, any code that wants to serialize an instance of this type should invoke the serializeJsonValue static method.
 *
 */
@JsonDeserialize(using = TWJson.TWJsonDeserializer.class)
public class TWJson {
	
	 final TreeNode actualJsonValue;
	 private final NullNode  aNullInstance = NullNode.getInstance();
	 public TWJson() {
		 actualJsonValue = aNullInstance;}
	 public TWJson(TreeNode value) {actualJsonValue = value;}
	 public TreeNode getActualJsonValue() {return actualJsonValue;}

	 static final String sqlNullSerialization = "*sqlnull*";
	 /*static void configureForMapper(JsonMapper mapper) {
		 var jnode = new SimpleModule();
		 jnode.addSerializer(TWJson.class, new TWJsonSerializer());
		 jnode.addDeserializer(TWJson.class, new TWJsonDeserializer());
	 }*/
	 
	 public static void serializeJsonValue(TWJson twjson, JsonGenerator gen) throws IOException {
		 if (twjson == null) {
			 var target = (OutputStream)gen.getOutputTarget();
			 target.write('"'); target.write(sqlNullSerialization.getBytes()); target.write('"'); 
		 } else ((ObjectMapper)gen.getCodec()).writeTree(gen, twjson.getActualJsonValue());
	 }


	 /*static class TWJsonSerializer extends JsonSerializer<TWJson> {
		@Override
		public void serialize(TWJson twj, JsonGenerator gen, SerializerProvider arg2) throws IOException {
			if (twj == null) gen.writeString(sqlNullSerialization);
			else ((ObjectMapper)gen.getCodec()).writeTree(gen, twj.getActualJsonValue());
		}
	 }*/

	 static class TWJsonDeserializer extends JsonDeserializer<TWJson>{
		@Override
		public TWJson deserialize(JsonParser parser, DeserializationContext arg1)	throws IOException {
			var jn = parser.readValueAsTree();
			if (jn instanceof TextNode  && ((TextNode)jn).asText().equals(sqlNullSerialization))
				return null;
			return new TWJson(jn);
		}
    }
}
