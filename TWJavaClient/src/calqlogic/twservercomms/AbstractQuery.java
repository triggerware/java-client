package calqlogic.twservercomms;

import java.util.Hashtable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.TypeFactory;

import nmg.softwareworks.jrpcagent.JRPCException;
import nmg.softwareworks.jrpcagent.Logging;

/**
 * A superclass for various forms of query for which TW provides a handle to allow the client to use the query.
 * In all cases, the handle is returned from some request on a connection, and the handle is only valid for requests
 * submitted on that connection.
 *
 * @param <T> the class that repesents a single 'row' of the answer to the query.
 */
/**
 * @author Neil
 *
 * @param <T>
 */
/**
 * 
 *
 * @param <T> the row type for the query result
 */
public abstract class AbstractQuery<T> implements Cloneable{
	

	static class ReregistrationError extends JRPCException{
		protected ReregistrationError () {
			super("attempt to register an already registered query");	}
	}

	protected Integer twHandle = null;
	protected boolean closed = false;
	protected final String query, language, schema;
	protected TriggerwareConnection connection = null;
	protected TriggerwareClient getClient() {return  connection==null? null : connection.getClient();}
	//protected final Class<T>rowType;
	//two of these will be null
	protected final Class<T> rowClass;
	protected final TypeReference<T> rowTypeRef;
	protected final JavaType rowJType;
	
	protected AbstractQuery (Object rowType, String query, String schema) {
		this(rowType, query, Language.SQL, schema);}

	@SuppressWarnings("unchecked")
	protected AbstractQuery (Object rowType, String query, String language, String schema) {
		this.query = query;
		this.schema = schema;
		this.language = language;
		if (rowType instanceof Class<?>) {
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
		} 
	}
	
	protected AbstractQuery(AbstractQuery<T> aq) { //copy constructor
		this.query = aq.query;
		this.schema = aq.schema;
		this.language = aq.language;
		this.rowClass = aq.rowClass;
		this.rowTypeRef = aq.rowTypeRef;
		this.rowJType = aq.rowJType;		
	}
	
	protected void recordRegistration(TriggerwareConnection connection, int twHandle) {
		this.connection = connection;
		this.twHandle = twHandle;
	}
	
	abstract boolean closeQuery();
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
	
	JavaType parametricTypeFor(TriggerwareClient client, Class<?>genericClass) {
		var tf = TypeFactory.defaultInstance();
		return (rowClass != null) ? tf.constructParametricType(genericClass,  rowClass)
									: tf.constructParametricType(genericClass, rowJType);
	}

	final static Hashtable<String, Class<?>> columnClasses = new Hashtable<String, Class<?>>();
	static {
		columnClasses.put("double", Double.TYPE);
		columnClasses.put("integer", Integer.TYPE);
		columnClasses.put("number", Number.class);
		columnClasses.put("boolean", Boolean.TYPE);
		columnClasses.put("stringcase", String.class);
		columnClasses.put("stringnocase", String.class);
		columnClasses.put("stringagnostic", String.class);
		columnClasses.put("date", java.time.LocalDate.class);
		columnClasses.put("time", java.time.LocalTime.class);
		columnClasses.put("timestamp", java.time.Instant.class);
		columnClasses.put("interval", java.time.Duration.class);
		columnClasses.put("", Object.class);
		// TODO:     Blob
	}
	static Class<?> classFromName(String className){
		return columnClasses.get(className.toLowerCase());}

	@JsonFormat(shape=JsonFormat.Shape.OBJECT)
	static class SignatureElement{
		final String name;
		final String typeName;
		@JsonCreator
		SignatureElement(@JsonProperty("attribute")String name, @JsonProperty("type")String typeName){
			this.name = name;
			//private  Class<?>[]  signatureTypes;  //private  String[] signatureNames, signatureTypeNames;
			this.typeName = typeName;
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
			var tp = classFromName(se.typeName);
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
