package nmg.softwareworks.jrpcagent;

import java.io.*;
//import java.util.Map;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.cfg.ContextAttributes;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

/*
 An ObjectMapper is where jackson holds the rules that determine the correspondence between the serialized form of Json (text)
 and the java object representation that is the source of serialization and the target of deserialization.
 
 You start by creating a MappingJsonFactory.  MappingJsonFactory has a zero-argument constructor.
 You then create a JsonMapper, using a 'builder' paradigm. 
 
 You can enable certain predefined features to do a slight amount of tailoring of the JsonMapper.
 
 You customize the JsonMapper by creating a SimpleModule (also with a zero argument constructor) and using addSerializer (ddDeserializer) to specify custom serialization for 
 types for which you want non-default serialization (deserialization) The custom serializers/deserializers that you add have methods that will be executed with access
 to the serialization context, so they can serialize/deserialize in a context-sensitive manner.

 You use the registerModule method of the JsonMapper to associate your module of custom rules with the JsonMapper.
 
 A JsonParser -- which deserializes Json text -- holds a  source of chararacters for deserializing.
 A JsonGenerator -- which serializes Json text -- holds a target (output character stream) for serializing.
 
 You create a JsonParser that uses your JsonMapper with the mapper's createParser methods.  The different overloads of this method allow you to use different sources of the
 json text to be deserialized (e.g., an InputStream)
 You create a JsonGenerator that uses your JsonMapper with the mapper's createGenerator methods.  The different overloads of this method allow you to use different targets of the
 json text to be serialized (e.g., an OutputStream)
 
 To parse from the input source there are numerous methods of JsonParser.  You can actually retrieve the input source (parser.getInputSource()) and consume input directly,
 but that is rarely useful. At the other extreme, you can use the parser's readValueAsTree to simply deserialize one Json "document" from the input source, which is returned as 
 an instance of the jackson interface type TreeNode.  That value will be an instance of a subclass of either ContainerNode or ValueNode. You can use the parser's readValueAs
 method to direct the parser to deserialize one Json document, directing it to use the rules for building a specific type of result. (E.g., if the input source is positioned
 at the start of a Json object, readValueAsTree would produce an ObjectNode, whereas readValueAs could be used to direct the parser to produce an instance of a specific class
 that is not a TreeNode at all.
 
 Jackson has two sorts of customization of a JsonMapper beyond the custom rules you register. One consists of a large number of predefined specializations of how serialization or deserialization
 will work in certain cases.  The other is through ContextAttributes.
 Jackson provides an abstract class named ContextAttributes. An instance of this class has key-value pairs. There is not generic type involved.
Both the keys and the values are arbitrary Objects. The consumer code generally know a specific key it wants to find the value fo, and needs to cast the value 
to make use of it. 
 When you build a JsonMapper, you can set its initial ContextAttributes with the defaultAttributes method.
 Whenever you have access to a ContextAttributes, you can with-xxx-Attribute methods to modify the key-value pairs.
 
 Inside the code of a custom serialization rule, your serialization code has access to an instance of SerializationProvider. This instance has a getAttribute method that
 can read the value associated with a key in the ContextAttributes of the JsonMapper controlling the serialization.
 
 A JsonMapper has both a DeserializationConfig and a DeserializationContext. Once you have a JsonMapper, you can customize both.
 A JsonMapper's DeserializationConfig is accessible with the method getDeserializationConfig.  The JsonMapper starts life with a default DeserializationConfig.
 DeserializationConfig is a final class, but an instance can be customized (mainly via with-xxx methods) to change deserialization behavior in may PREDEFINED ways.
 
 A JsonParser
 
 A JsonMapper's DeserializationContext is accessible with the method getDeserializationContext.  The JsonMapper starts life with a default DeserializationContext.
 In addition to methods for controlling deserialization in some PREDEFINED ways, a DeserializationContext
 
 
 Jackson provides a final class SerializationConfig. This object can be configured using a builder-like api (with-xxx methods) to control serialization in certain predefined 
 ways. It can be given arbitrary application-specific properties (ContextAttributes) that custom serialization rules can use to control their output.
 
 Custom serializers in Jackson have a method named serialize that accepts a parameter of type SerializerProvider.
 
   An instance of this class is associated with each JsonParser
 and with each JsonGenerator.
  Jackson provides an abstract class DeserializationConfig.
 
 Custom deserializers use a method named deserialize that accepts a parameter of type DeserializationContext.
 
 */

public class JsonUtilities {
	//may need to run java with command line args
	// --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED
	static final JsonNodeFactory jnfactory = JsonNodeFactory.instance;
	static final MappingJsonFactory mjfactory =  new MappingJsonFactory();
	//static final SimpleModule JRPCRules = new SimpleModule(); //currently empty, but should have something for messages, no?
	public static final JsonMapper jsonMapper(SerializationState deserializationState) { 
		var defaultDsAtts = ContextAttributes.getEmpty().withSharedAttribute("deserializationState", deserializationState);
		var mapper = JsonMapper.builder(mjfactory) .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
				.defaultAttributes(defaultDsAtts) //.constructorDetector(ConstructorDetector.USE_PROPERTIES_BASED)
				.build();
		//JsonTimeUtilities.isoSerialization(mapper);
		//mapper.registerModule(JRPCRules);
		return mapper;
	}
	public static void addStandardJRPCProperties(JsonGenerator jg, String methodName) throws IOException {
		jg.writeStringField("jsonrpc", "2.0");
		if (methodName!=null) jg.writeStringField("method", methodName);
	}
		
	static JsonGenerator createStreamSerializer(JsonMapper mapper, LoggingWriter oWriter) throws IOException {
		return mapper.createGenerator(oWriter);}
	static JsonParser createStreamDeserializer(JsonMapper mapper, LoggingReader iReader)throws IOException {
		var parser = mapper.createParser(iReader);
		parser.setCodec(mapper);
		//parser.enable( JsonReadFeature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER);
		return parser;
	}

	//public static <T> JRPCSimpleRequest<T> createRequest(JRPCAgent agent, boolean clientAsynchronous, //Map<String, TreeNode> meta,
	//		Object resultType,  String methodName, Object parameters){
	//	return new JRPCSimpleRequest<T>( agent, clientAsynchronous, /*meta,*/ resultType,  methodName, parameters );	}
	/*public static <T> JRPCSimpleRequest<T> createRequest(OutboundRequest<T> outbound, JRPCAgent agent, boolean clientAsynchronous,
			//Map<String, TreeNode> meta,
			  Object resultType,   //String methodName,
			    Object parameters) {
		return clientAsynchronous ? new JRPCAsyncRequest<T>(outbound, agent, //meta,
				outbound.methodName,  resultType,  parameters) 
		: new JRPCSimpleRequest<T>(outbound, agent, //meta,
		 	outbound.methodName,  resultType, parameters);
	}
	public static <T> JRPCSimpleRequest<T> createRequest(OutboundRequest<T> outbound, JRPCAgent agent, boolean clientAsynchronous,
									//Map<String, TreeNode> meta,
 							Object resultType,   //String methodName,
 							 Object parameters) {
	    return clientAsynchronous ? new JRPCAsyncRequest<T>(outbound, agent, //meta, methodName, 
	     resultType,  parameters) 
	    		: new JRPCSimpleRequest<T>(outbound, agent, //meta, methodName,
	    		 resultType, parameters);
	}*/
	
	public abstract static class FieldHandler {
		public abstract void badJsonSyntax(); //where an attribute name or object end should have appeared, something else showed up
		public abstract void processFieldValue(String attributeName) throws IOException;
	}
	
	public static void mapObjectFields(JsonParser jParser, FieldHandler fieldHandler) throws IOException {
		while (true) {//parse individual fields
			var fieldName = jParser.nextFieldName();
			if (fieldName==null) { 
				var token = jParser.currentToken();
				if (token == JsonToken.END_OBJECT) 	break; //normal end
				fieldHandler.badJsonSyntax();
				break;
			}
			jParser.nextToken();
			fieldHandler.processFieldValue(fieldName);
		}		
	}
}
