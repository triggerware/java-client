package nmg.softwareworks.jrpcagent;

import java.io.*;
import java.lang.reflect.Array;
import java.util.Map;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.*;

/*
 An ObjectMapper is where jackson holds the rules that determine the correspondence between the serialized form of Json (text)
 and the java object representation that is the source of serialization and the target of deserialization
 
 A JsonParser holds a  source of chararacters for deserializing.
 A JsonGeneteror holds a target (output character stream) for serializing.
 
 Parsers and generators are created by MappingJsonFactory instances.
 
 In order to deserialize, you need both a JsonParser and an ObjectMapper
 In order to serialize, you need both a JsonGenerator and an ObjectMapper
 */

public class JsonUtilities {
	private static final JsonNodeFactory jnfactory = JsonNodeFactory.instance;
	private static final MappingJsonFactory mjfactory = new MappingJsonFactory();

	static final ArrayNode emptyArray() {return jnfactory.arrayNode();}
	/*private static void addStandardJRPCProperties(JsonObjectBuilder obuilder, String methodName) {
		obuilder.add("jsonrpc", "2.0");
		obuilder.add("method", methodName);
	}
	public static void addStandardJRPCProperties(ObjectNode msg, String methodName) {
		msg.put("jsonrpc", "2.0");
		if (methodName!=null) msg.put("method", methodName);
	}*/

	public static void addStandardJRPCProperties(JsonGenerator jg, String methodName) throws IOException {
		jg.writeStringField("jsonrpc", "2.0");
		if (methodName!=null) jg.writeStringField("method", methodName);
	}
	
	//private static JsonParser createParser (LoggingReader r) throws JsonParseException, IOException {
	//	return mjfactory.createParser(r);}

	public static class JRPCGenerator {
		private final LoggingWriter w;
		private final JsonGenerator generator;
		private final JRPCObjectMapper mapper;
		JRPCGenerator (LoggingWriter w, JRPCObjectMapper mapper) throws IOException {
			this.w = w;
			this.mapper = mapper;
			generator = mjfactory.createGenerator(w);
			generator.setCodec(mapper);
			generator.enable(JsonGenerator.Feature.FLUSH_PASSED_TO_STREAM);
		}
		
		JsonGenerator getGenerator() {return generator;}
		void startLogging() { w.setLogging(true);}
		String logEntryComplete() { 
			return w.getLoggedText(true);}

		void streamMeta(Map<String, TreeNode> meta) throws IOException {
			if (meta!=null) {
				for (var pair: meta.entrySet())
					streamAttributeValue(pair.getKey(),  pair.getValue());
			}
		}
		void writeWithSerializationState(JRPCAgent agent, Object value, Map<String,TreeNode>meta)  throws IOException {
			var old = agent.prepareSerializationState(mapper, meta, value);
			IOException thrown = null;
			try {
				mapper.writeValue(generator, value);
			}catch (IOException e) {thrown = e;
			} finally {
				agent.restoreSerializationState(mapper, old, value);
				if (thrown != null) throw (thrown);}			
		}
		void streamResponseResultOrData(String attribute, Object value, Map<String,TreeNode>meta) throws IOException  {
			var isResult = attribute.equals("result");
			var agent = mapper.getConnection().getAgent();
			generator.writeFieldName(attribute);
			writeWithSerializationState(agent, value, isResult ? meta : null);
		}
		void streamAttributeValue(String attribute, Object value) throws IOException  {
			generator.writeFieldName(attribute);
			mapper.writeValue(generator, value);
		}
		JRPCObjectMapper getMapper() {return mapper;}
		void addStandardJRPCProperties(String methodName) throws IOException {
			JsonUtilities.addStandardJRPCProperties(generator, methodName);	}
	}
	/*public void serialize(Object v, ObjectMapper om, OutputStream s) throws IOException {
		var gen = mjfactory.createGenerator(s);
		om.writeValue(gen, v);
	}*/
	
	public interface ISerializationState{
		Object setState(Object desiredState,  boolean topLevel);
		void restoreState(Object howToRestore, boolean topLevel);
	}
	
	//static DefaultDeserializationContext jrpcDeserializationContext = 
	/**
	 * A JRPCObjectMapper is an ObjectMapper configured for serializing/deserializing dates and times
	 * using the ISO standard textual representation
	 * It belongs to a specific JRPCAgent;
	 * @author nmg
	 *
	 */
	public static class JRPCObjectMapper extends JsonMapper{
		private final Connection connection;
		protected ISerializationState serState = null, deserState = null;
		public JRPCObjectMapper() {this(null);}
		public JRPCObjectMapper(Connection connection) {
			super(mjfactory);
			//registerModule(new JavaTimeModule());
			//disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
			JsonTimeUtilities.isoSerialization(this);
			this.connection = connection;
			/*var scfg = getSerializationConfig();
			var cx= scfg.getAttributes().withSharedAttribute("serializationState",null);
			setConfig(scfg.with(cx));
			var dscfg = getDeserializationConfig();
			cx= dscfg.getAttributes().withSharedAttribute("deserializationState", null);
			setConfig(dscfg.with(cx));*/
		}
		public Connection getConnection() {return connection;} //is this ever needed?
		public final ISerializationState getSerializationState() {return serState;}

        public final void setSerializationState(ISerializationState iss) {
			var scfg = getSerializationConfig();
			var cx= scfg.getAttributes().withSharedAttribute("serializationState", iss);
			setConfig(scfg.with(cx));
			serState = iss;
		}
		public final ISerializationState getDeserializationState() {return deserState;}

        public final void setDeserializationState(ISerializationState ids) {
			var dscfg = getDeserializationConfig();
			var cx= dscfg.getAttributes().withSharedAttribute("deserializationState", ids);
			setConfig(dscfg.with(cx));
			deserState = ids;
		}
		//public Object establishState(Object value) {	return null;	}

		//public Object establishState() {return null;}
	}
		
		
	static JRPCGenerator createStreamSerializer(OutputStream ostream, JRPCObjectMapper mapper) throws IOException {
		return new JRPCGenerator(new LoggingWriter(ostream), mapper);}
	static JsonParser createStreamDeserializer(InputStream istream, ObjectMapper mapper)throws IOException {
		var parser = mjfactory.createParser(new LoggingReader(istream));
		parser.setCodec(mapper);
		return parser;
	}

	/*public <TObj> TObj deserialize(ObjectNode parsed, TObj instance) throws IOException  {
		var reader = twMapper.readerForUpdating(instance);
		TObj t = reader.readValue(parsed);
		//this.notAStub();
		return t;
	}*/

	private static Class<?> toWrapper(Class<?> clazz) {//amazing that java doesn't have a method for this.
		assert clazz.isPrimitive();
	    if (clazz == Long.TYPE)     return Long.class;
	    if (clazz == Integer.TYPE)  return Integer.class;
	    if (clazz == Double.TYPE)   return Double.class;
	    if (clazz == Boolean.TYPE)  return Boolean.class;
	    if (clazz == Byte.TYPE)     return Byte.class;
	    if (clazz == Character.TYPE)return Character.class;
	    if (clazz == Float.TYPE)    return Float.class;
	    if (clazz == Short.TYPE)    return Short.class;
	    if (clazz == Void.TYPE)     return Void.class;
	    return clazz;
	}

	private static Number numericConversion(Class<? extends Number>target, Number n) {
		if (target == Long.class) return n.longValue();
		if (target == Double.class)	return n.doubleValue();
		if (target == Integer.class) return n.intValue();
		if (target == Float.class)	return n.floatValue();
		if (target == Short.class) return n.shortValue();
		if (target == Byte.class) return n.byteValue();
		return n;// should never occur
	}

	@SuppressWarnings("unchecked")
	private static Object deserializePrimitive(JsonNode jn, Class<?>targetClass) {
		//assert jn.isValueNode();
		var num = jn.numberValue();
		if (num!=null) {
			if (targetClass.isPrimitive()) 
				targetClass = toWrapper(targetClass);
			if (targetClass.isInstance(num)) return num;
			if (Number.class.isAssignableFrom(targetClass) ) 
				return numericConversion((Class<? extends Number>)targetClass, num);
			return num;
		}
		if (jn.isTextual()) return jn.textValue();
		if (jn.isBoolean()) return jn.asBoolean();
		if (jn.isNull()) return null;
		return null;
	}
	static Object deserialize (JsonNode jn, Class<?>targetClass) {
		if (jn.isValueNode()) return deserializePrimitive(jn,targetClass);
		if (targetClass.isArray() || targetClass == Object.class) {
			var elementType = targetClass == Object.class ? targetClass : targetClass.getComponentType();
			if (jn.isNull()) // because Don's serializer may produce null nodes where it should produce []?
				return Array.newInstance(elementType, 0);
			if (jn.isArray()) {
				var ja = (ArrayNode)jn;
				var result = Array.newInstance(elementType, ja.size());
				int index = 0;
				for (var jv : ja)
					Array.set(result, index++, deserialize(jv, elementType));
				return result;				
			}
		}
		return null;
	}
	
	/*public static Object convertFromTree(TreeNode tree, JavaType jt, ObjectMapper om) throws IOException {
		//craziness -- only way I can find to parse using a JavaType as the target
		// because there is no method for that in JsonParser
		var baos = new java.io.ByteArrayOutputStream();
		var gen = createGenerator(new OutputStreamWriter(baos));
		gen.writeTree(tree); gen.close();
		var bytes = baos.toByteArray();
		return om.readValue(bytes, jt);
	}*/
	
	//public static JsonNode bserialize(Object param, Class<?>targetType) {return bserialize(param);}
	/*static JsonNode bserialize(JRPCObjectMapper om, Object param) {
		if (param == null) 	return NullNode.instance;
		var cls = param.getClass(); //TODO: is there enough specific dispatch by value of cls to warrant a hash map
		if (cls == String.class) return  new TextNode((String)param);
		if (param instanceof Number) { 
			if (cls == Integer.class) return new IntNode((Integer)param);
			if (cls == Double.class) return new DoubleNode((Double)param);
			if (cls == Long.class) return new LongNode((Long)param);
			if (cls == Short.class) return new ShortNode((Short)param);
			if (cls == Float.class) return new FloatNode((Float)param);
			if (cls == Byte.class) return new ShortNode((Byte)param);
			if (cls == BigInteger.class) return new BigIntegerNode((BigInteger)param);
			if (cls == BigDecimal.class) return new DecimalNode((BigDecimal)param);
		}
		if (cls == Boolean.class) return (Boolean)param ?  BooleanNode.TRUE : BooleanNode.FALSE;
		if (cls == Character.class) return new TextNode(((Character)param).toString());
		
		if (cls == LocalTime.class) return new TextNode(JsonTimeUtilities.asJson((LocalTime) param));
		if (cls == LocalDate.class) return new TextNode(JsonTimeUtilities.asJson((LocalDate) param));
		if (cls == OffsetDateTime.class) return new TextNode(JsonTimeUtilities.asJson((OffsetDateTime) param));
		
		if (cls.isArray()) {
			var arrayParam = jnfactory.arrayNode();
			var elements = (Object[]) param;
			for (var element : elements) {
				//JsonNode jn = null;
				arrayParam.add(bserialize(element));
			}
			return arrayParam;
		} 
		if (param instanceof JsonNode)
			return (JsonNode)param;
		if (param instanceof NamedRequestParameters){
			var objParam = jnfactory.objectNode();
			for (var pair :((NamedRequestParameters) param).entrySet())
				objParam.set(pair.getKey(), bserialize(pair.getValue()));
			return objParam;
		}
		{// TODO: some runtime error			
			return null;
		}		
	}*/
	/*private static void addParameter(ObjectMapper mapper, ArrayNode params, Object param) {
		var jn = jsonNodefromJava(param);
		// temporary -- handles only null, String, primitives, and arrays (recursively) thereof
		if (param == null) {
			params.add(NullNode.instance);
			return;
		}
		var cls = param.getClass();
		if (cls == String.class) {
			params.add((String)param);
		} else if (cls == Integer.class) {
			params.add((Integer)param);
		} else if (cls == Double.class) {
			params.add((Double)param);
		} else if (cls == Long.class) {
			params.add((Long)param);
		} else if (cls == Short.class) {
			params.add((Short)param);
		} else if (cls == Float.class) {
			params.add((Float)param);
		} else if (cls == Boolean.class) {
			params.add((Boolean)param);
		} else if (cls == Character.class)  {
			params.add(((Character)param).toString());
		} else if (cls.isArray()) {
			var arrayParam = mapper.createArrayNode();
			var elements = (Object[]) param;
			for (var element : elements)
				addParameter(mapper, arrayParam, element);
			params.add(arrayParam);
		} else if (param instanceof JsonNode)
			params.add((JsonNode)param);
		else {// TODO: some runtime error
			
		}
	}*/
	
	
	
	/*static void streamValue(JsonGenerator jg, Object value) throws IOException {
		// temporary -- handles only null, String, primitives, and arrays (recursively) thereof
		if (value == null) 	jg.writeNull();
		//jg.getOutputTarget()
		else {
			var cls = value.getClass();
			if (cls == String.class) jg.writeString((String)value);
			else if (value instanceof Number) {
				if (cls == Integer.class) jg.writeNumber((Integer)value);
				else if (cls == Double.class) jg.writeNumber((Double)value);
				else if (cls == Long.class) jg.writeNumber((Long)value);
				else if (cls == Short.class) jg.writeNumber((Short)value);
				else if (cls == Float.class) jg.writeNumber((Float)value);
				else if (cls == Byte.class) jg.writeNumber((Byte)value);
				else if (cls == BigInteger.class) jg.writeNumber((BigInteger)value);
				else if (cls == BigDecimal.class) jg.writeNumber((BigDecimal)value);
			}
			else if (cls == Boolean.class)   jg.writeBoolean((Boolean)value);
			else if (cls == Character.class) jg.writeString(((Character)value).toString()); 
			else if (cls.isArray()) {
				jg.writeStartArray();
				var elements = (Object[]) value;
				for (var element : elements) streamValue(jg, element);
				jg.writeEndArray();
			} else if (value instanceof TreeNode) jg.writeTree((TreeNode)value);
			else {// TODO: some runtime error
			}		
		}		
	}*/

	public static <T> JRPCSimpleRequest<T> createRequest( JRPCAgent agent, boolean clientAsynchronous, Map<String, TreeNode> meta,
			Object resultType, T resultInstance, String methodName, Object parameters){
		return createRequest(null, agent, clientAsynchronous, meta, resultType, resultInstance, methodName, parameters );	}
	public static <T> JRPCSimpleRequest<T> createRequest(OutboundRequest<T> outbound, JRPCAgent agent, boolean clientAsynchronous,
									Map<String, TreeNode> meta, Object resultType, T resultInstance, String methodName, Object parameters) {
	    return clientAsynchronous ? new JRPCAsyncRequest<T>(outbound, agent, meta, methodName, resultInstance, resultType, parameters) 
	    		: new JRPCSimpleRequest<T>(outbound, agent, meta, methodName, resultInstance, resultType, parameters);
	}
	
	public abstract static class FieldHandler {
		public abstract void badJsonSyntax(); //where an attribute name or object end should have appeared, something else showed up
		public abstract void processFieldValue(String attributeName) throws IOException;
	}
	
	public static void mapObjectFields(JsonParser jParser, FieldHandler fieldHandler) throws IOException {
		while (true) {//parse individual fields
			var fieldName = jParser.nextFieldName();
			if (fieldName==null) { 
				var token = jParser.getCurrentToken();
				if (token == JsonToken.END_OBJECT) 	break; //normal end
				fieldHandler.badJsonSyntax();
				break;
			}
			jParser.nextToken();
			fieldHandler.processFieldValue(fieldName);
		}		
	}
}
