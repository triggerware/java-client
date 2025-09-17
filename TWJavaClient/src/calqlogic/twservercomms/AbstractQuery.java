package calqlogic.twservercomms;

import java.io.Closeable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.type.TypeFactory;

import nmg.softwareworks.jrpcagent.JRPCException;
import nmg.softwareworks.jrpcagent.Logging;

/**
 * A superclass for various forms of query for which TW provides a handle to allow the client to use the query.
 * In all cases, the handle is returned from some request on a connection, and the handle is only valid for requests
 * submitted on that connection.
 *
 * @param <T> the type that repesents a single 'row' of the result of the query.
 */

public abstract class AbstractQuery<T> implements Cloneable, Closeable{
	

	static class ReregistrationError extends JRPCException{
		protected ReregistrationError () {
			super(String.format("attempt to register an already registered query"));	}
	}

	protected Integer twHandle = null;
	protected boolean closed = false;
	protected final String query, language, schema;
	protected TriggerwareConnection connection = null;
	protected TriggerwareClient getClient() {return  connection==null? null : connection.getClient();}
	//two of these will be null
	protected final Class<T> rowClass; //rhis value is used to construct a jackson parametric type for deserializing rows
	//protected final TypeReference<T> rowTypeRef;
	//protected final JavaType rowJType;
	protected final Constructor<T> rowConstructor;
	
	protected AbstractQuery (Class<T> rowClass, String query, String schema) {
		this(rowClass, query, Language.SQL, schema);}

	protected AbstractQuery (Class<T> rowClass, String query, String language, String schema) {
		this.query = query;
		this.schema = schema;
		this.language = language;
		this.rowClass = rowClass;
		this.rowConstructor = getRowConstructor(rowClass);
		/*if (rowType instanceof Class<?>) {
			rowClass = (Class<T>) rowType;
			rowTypeRef = null;
			rowJType = null;
		} else if (rowType instanceof TypeReference<?>){
			rowClass = null;
			rowTypeRef = (TypeReference<T>) rowType;
			rowJType = null;
		} else {//uses a JavaType
			rowClass = null;
			rowTypeRef = null;
			rowJType = (JavaType) rowType;
		} */
	}
	
	protected AbstractQuery(AbstractQuery<T> aq) { //copy constructor
		this.query = aq.query;
		this.schema = aq.schema;
		this.language = aq.language;
		this.rowClass = aq.rowClass;
		//this.rowTypeRef = aq.rowTypeRef;
		//this.rowJType = aq.rowJType;
		this.rowConstructor = aq.rowConstructor;
	}
	
	protected void recordRegistration(TriggerwareConnection connection, int twHandle) {
		this.connection = connection;
		this.twHandle = twHandle;
	}
	
	/**
	 * @return the connection on which this query is used.
	 */
	public TriggerwareConnection getConnection() {return connection;}
	/**
	 * @return the query string
	 */
	public String getQuery() {return query;}
	/**
	 * @return the query's default schema
	 */
	public String getSchema() {return schema;}
	protected Integer getHandle() {return twHandle;}
	
	JavaType parametricTypeFor(/*TriggerwareClient client,*/ Class<?>genericClass) {
		var tf = TypeFactory.defaultInstance();
		return tf.constructParametricType(genericClass,  rowClass);
	}
	
	@SuppressWarnings("unchecked")
	static <T>Constructor<T> getRowConstructor(Class<T> rowClass){
		if (rowClass ==  Object[].class) return null;
		for (var con : rowClass.getConstructors()) {
			if (con.isAnnotationPresent(DeserializationConstructor.class) && Modifier.isPublic(con.getModifiers()))
				return (Constructor<T>)con;
		}
		return null;
	}

	@JsonFormat(shape=JsonFormat.Shape.OBJECT)
	static class SignatureElement{
		final String name;
		final String typeName;
		@JsonIgnore
		final Class<?> twSqlType;
		//temporary
		/*static SignatureElement[]fromTree(ArrayNode cols){
			var sig = new SignatureElement[cols.size()];
			var index=0;
			for (var jcol : cols) {
				var ocol = (ObjectNode)jcol;
				sig[index++] = new SignatureElement(ocol.get("attribute").asText(), ocol.get("type").asText());
			}
			return sig;
		}*/
		/*@JsonCreator //is this ever used??
		SignatureElement(@JsonProperty("attribute")String name, @JsonProperty("type")Class<?> twSqlType){
			this.name = name;
			this.typeName = null;
			this.twSqlType = twSqlType;
			//Logging.log("col=%s type=%s",name,twSqlType);
		}*/
		public SignatureElement(@JsonProperty("attribute")String name, @JsonProperty("type")String typeName){
			this.name = name;
			this.typeName = typeName;
			this.twSqlType = TWBuiltInTypes.classFromName(typeName);
		}
	}
	
	static String[] signatureNames(SignatureElement[] sig){
		var result = new String[sig.length];
		int index = 0;
		for(var se : sig) {
			var paramName = se.name;
			if (paramName.charAt(0) == '?') paramName = paramName.substring(1);
			result[index++] =  paramName;
		}
		return result;			
	}

	static Class<?>[] typeSignatureTypes(SignatureElement[] sig){
		var result = new Class<?>[sig.length];
		int index = 0;
		for (var se : sig) {
			var tp = se.twSqlType;//TWBuiltInTypes.classFromName(se.typeName);
			result[index++] = tp;
			if (tp == null) Logging.log("unknown type name from TW <%s>", se.typeName);
		}
		return result;
		
	}

	static String[] typeSignatureTypeNames(SignatureElement[] sig){
		var result = new String[sig.length];
		int index = 0;
		for (var se : sig)
			result[index++] = se.typeName;
		return result;			
	}
	
	/**
	 * @return true if this query has been registered with the TW server.
	 */
	public boolean isRegistered() {return twHandle!=null;}
	
	/**
	 * Override this method in a subclass if your application asks for query results to be returned by notification.
	 * @param resultSet  the resultset sent in a notification
	 */
	public void processNotificationResultset(TWResultSet<T> resultSet){
		
	}
	
	/**
	 * Override this method in a subclass if your application asks for query results to be returned by notification.
	 * @param error  the error sent in a notification
	 */public void processNotificationError(JsonValue error){
		
	}

}
