package calqlogic.twservercomms;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashMap;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.type.TypeFactory;

import calqlogic.twservercomms.AbstractQuery.SignatureElement;
import calqlogic.twservercomms.TriggerwareClient.TriggerwareClientException;
import nmg.softwareworks.jrpcagent.*;

/**
 * A statement than can be used to issue query requests on a connection.
 * The statement may be executed {link #executeQuery} multiple times on the connection.
 * A QueryStatement may also be executed using {link #executeQueryWithNotificationResults} to have the rows 'streamed' back to
 * the client on the connection.
 *
 */
public class QueryStatement implements Statement{
	
	private Integer fetchSize;
	private final TriggerwareConnection connection;
	private TWResultSet<?> resultSet = null ; 
	private final TypeFactory typeFactory;
	
	/**
	 * create a new query statement on a clients primary connection
	 * @param client the client that will use this QueryStatement
	 */
	protected QueryStatement(TriggerwareClient client) {
		this(client.getPrimaryConnection());}

	/**
	 * create a new query statement
	 * @param connection the connection on which the query will be issued.
	 */
	protected QueryStatement(TriggerwareConnection connection) {
		this.connection = connection;
		this.typeFactory = connection.getTypeFactory();
		this.fetchSize = ((TriggerwareClient)(connection.getAgent())).getDefaultFetchSize();
	}
	
	//protected void setSignatureFree(boolean signatureFree) {this.signatureFree = signatureFree;}
	public void establishResponseDeserializationAttributes(JRPCSimpleRequest<?> request, IncomingMessage response, DeserializationContext ctxt) {}

	/** 
	 * set the fetchSize  for executions of this QueryStatement.  The fetch size is the number of results to return in
	 * a ResultSet from executing the query, as well as the initial setting of that ResultSet's own fetchSize.
	 * @param fetchSize the fetchSize to use
	 * @return the previous setting of fetchSize for this QueryStatement
	 */
	public Integer setFetchSize(Integer fetchSize) {
		var old = this.fetchSize;
		this.fetchSize = fetchSize;
		return old;
	}

	@JsonFormat(shape=JsonFormat.Shape.OBJECT)
	@JsonDeserialize(using = ResultSetResult.RSRDeserializer.class)
	static class ResultSetResult<T> {
		public Integer handle;
		@JsonDeserialize(using = SignatureDeserializer.class)
		public final SignatureElement[] rowSignature;
		public final Batch<T> batch;
		public Constructor<T> rowConstructor;
		TWResultSet<T> resultSet;
		Integer getHandle() {return handle;}
		Batch<T>getBatch(){return batch;}
		@JsonCreator 
		public ResultSetResult(@JsonProperty(value = "handle", required = false) Integer handle, @JsonProperty("signature")SignatureElement[] signature,
						 @JsonProperty(value = "batch", required = false) Batch<T> batch){
			 this.handle = handle;
			 this.batch = batch;
			 this.rowSignature = signature;
		 }
		SignatureElement[]getSignature(){ return rowSignature;}
		Constructor<T> getRowConstructor(){return rowConstructor;}
		void setResultSet(TWResultSet<T> resultSet) {this.resultSet = resultSet;}
		TWResultSet<T> getResultSet() {return resultSet;};
		void setRowConstructor(Constructor<T> rowConstructor) {this.rowConstructor = rowConstructor;}
		
		static class RSRDeserializer<T> extends JsonDeserializer<ResultSetResult<T>>{

			@SuppressWarnings("unchecked")
			@Override
			public ResultSetResult<T> deserialize(JsonParser jParser, DeserializationContext ctxt)
					throws IOException, JacksonException {
				//var mapper = (ObjectMapper)jParser.getCodec();
				var dsstate = (HashMap<String,Object>)ctxt.getAttribute("deserializationState");
				var tkn = jParser.currentToken();
				if (tkn != JsonToken.START_OBJECT) {
					var whatIsIt = jParser.readValueAsTree();
					throw new IOException(String.format("ad hoc query result not serialized as a json object <%s>", whatIsIt));
				} else {
					//tkn = jParser.nextToken();
					Integer handle = null;
					SignatureElement[]signature = null;
					Batch<T>batch = null;
					while (true) {//parse individual fields
						var propertyName = jParser.nextFieldName();
						if (propertyName==null) { 
							tkn = jParser.currentToken();
							if (tkn == JsonToken.END_OBJECT) break;
							Logging.log("unexpected end of message on input stream");
							throw new IOException("ad hoc query result not serialized properly");
						}
						jParser.nextToken(); // now at the token following the field name
						switch (propertyName) {
						case "handle" ->{
							handle = jParser.readValueAs(Integer.class);
							break;
							}
						case "signature" ->{
							//temporary code
							//var jsig = (ArrayNode)jParser.readValueAsTree();
							//signature = SignatureElement.fromTree(jsig);
							signature = jParser.readValueAs(SignatureElement[].class);
							dsstate.put("rowSignature", signature);
							}
						case "batch" ->{
							if (!dsstate.containsKey("rowSignature") && dsstate.containsKey("FOL")) {
								dsstate.put("rowSignature", BatchRows.FOLSignature);}
							ObjectMapper mapper = (ObjectMapper) jParser.getCodec();
							batch = mapper.readValue(jParser,Batch.class);
							break;
							}
						default ->{
							@SuppressWarnings("unused")
							var val = jParser.readValueAsTree();
							throw new IOException(String.format("unexpected key <%s> in the result of an ad hoc query execution", propertyName));
							}
						}
					}
					return new ResultSetResult<T>(handle, signature, batch);
				}
			}
		}
	}

	private static class SignatureDeserializer extends JsonDeserializer<SignatureElement[]> {
		//this is just using the default deserialization, but adding the result to the deserialization state for use with 
		//deserialization of the batch property that comes later
		@Override
		public SignatureElement[] deserialize(JsonParser jparser, DeserializationContext ctxt)
				throws IOException, JacksonException {
			//var sstate = (JRPCSerializationState)(ctxt.getAttribute("serializationState"));
			var mapper = (ObjectMapper)jparser.getCodec();
			SignatureElement[] sig = mapper.readValue(jparser, SignatureElement[].class);
			@SuppressWarnings("unchecked")
			var dsstate= (HashMap<String,Object>)ctxt.getAttribute("deserializationState");
			dsstate.put("rowSignature", sig);
			return sig;
		}
		
	}
	
	/* from donc
optional:
 - limit: a maximum number of rows to generate
 - timelimit: a maximal amount of time before reporting the results found
   up to that time
 - notify-limit: a maximum number of unsent results to collect before
   reporting them
 - notify-timelimit: a maximum amount of time between finding a result and
   reporting it (and any other rows that have been found since
	 */

	/**
	 * QueryResourceLimits holds resource limits that the TW server will obey when computing an answer to a query.
	 * This object can be used with any request that returns a TWResultSet.
	 * Currently there are two kinds of limit:
	 * <ul>
	 * <li> a time limit, measured in seconds.  A null value means no timeout will be used.
	 * When a timeout is exceeded on the server, the result set returned may contain fewer rows than requested.</li>
	 * <li> a rowCountLimit, which is a limit on the <em>total</em> number of rows that will be returned in batches
	 * before the TW server will regard the results as exhausted.  The default is NULL, meaning no limit is imposed 
	 * by the QueryResourceLimits.  
	 * </ul>
	 */
	public static class QueryResourceLimits{
		private Integer rowCountLimit = null;
		private Double timeout = null;
		/**
		 * create a new QueryResourceLimits with default limits.
		 * No timeout, and no limit on the number of row values to be returned.
		 */
		public QueryResourceLimits() {}
		
		/**
		 * set the limit on the number of rows in the batch returned from a request
		 * @param limit the limit to set 
		 * @return this QueryResourceLimits instance
		 */
		public QueryResourceLimits withLimit(Integer limit) {
			if (limit != null && limit < 0)
			  throw new IllegalArgumentException(
					  "limit value passed to QueryResourceLimits.withLimit must be null or non-negative");
			this.rowCountLimit = limit;
			return this;
		}
		
		/**
		 * set the limit on the number of rows in the batch returned from a request
		 * @param timeout  a limit (in seconds) of the amount of time to expend on finding results
		 * @return this QueryResourceLimits instance
		 */
		public QueryResourceLimits withTimeLimit(Double timeout) {
			if (timeout != null && timeout <= 0.0)
			  throw new IllegalArgumentException(
					  "limit value passed to QueryResourceLimits.withTimeLimit must be null or positive");
			this.timeout = timeout;
			return this;
		}

		public Integer getRowCountLimit() {return rowCountLimit;}
		public Double getTimeout() {return timeout;}
	}
	
	private class AdHocQueryRequest<T> extends NamedParameterRequest<ResultSetResult<T>>{ // T is the row class
		private final Constructor<T> rowConstructor;
		@SuppressWarnings("unchecked")
		AdHocQueryRequest(JavaType responseType, Class<?>rowClass){
			super(responseType, /*null,*/ "execute-query",  null, null);
			rowConstructor = (Constructor<T>) AbstractQuery.getRowConstructor(rowClass);
		}
		Constructor<T> getRowConstructor(){return rowConstructor;}
		@Override
		public void establishResponseDeserializationAttributes(JRPCSimpleRequest<?> request, IncomingMessage response, DeserializationContext ctxt) {
			QueryStatement.this.establishResponseDeserializationAttributes(request,  response,  ctxt);
			if (rowConstructor != null) {
				@SuppressWarnings("unchecked")
				var dsstate= (HashMap<String,Object>)ctxt.getAttribute("deserializationState");
				dsstate.put("rowBeanConstructor", rowConstructor);
			}
		}
	}
	
	/**
	 * execute an ad-hoc SQL query, returning a resultset 
	 * Object[] is used as the row type, meaning that each row element in a result is deserialized from json using the default interpretation
	 * as a Java object. No resource usage limits are imposed;all rows are returned in  the first resultset batch.
	 * @param query  the SQL query string
	 * @param schema the schema to use for resolving table and function names in the query string
	 * @return a TWResultSet with the first batch of results from executing this PreparedQuery
	 * @throws JRPCException a variety of problems -- communications, protocol, serialization/deserialization, or method-specific
	 * @throws TriggerwareClientException if this QueryStatement is closed 
	 */
	/*public  TWResultSet<Object[]> executeQuery(String query,  String schema)
			throws JRPCException, TriggerwareClientException{
		var qrl = fetchSize==null? null : new QueryResourceLimits().withLimit(fetchSize);
		return executeQuery(Object[].class, query, Language.SQL, schema, qrl);	
	}*/
	
	
	
	private void commonCheck() throws TriggerwareClientException {
		if (closed) throw  new TriggerwareClientException("attempt to execute a closed QueryStatement");
		if (resultSet != null) {
			resultSet.close();
			resultSet = null;
		}		
	}
	
	protected NamedRequestParameters commonParams(String query, String schema) {
		return new NamedRequestParameters().with("query", query).with("language", Language.SQL).with("namespace", schema)
				.with("check-update", false);
	}

	/**execute an ad-hoc query.
	 * Other executeQuery methods simple provide default values for one or more parameters of this method.
	 * @param <T> the row type for the results
	 * @param rowClass the class of the row type
	 * @param query  the query string
	 * @param schema the schema name to use for interpreting the query string
	 * @param qrl resource limits on executing the query. null means no limits.
	 * @return a TWResultSet with the first batch of results from executing this query
	 * @throws JRPCException a variety of problems -- communications, protocol, serialization/deserialization, or method-specific
	 * @throws TriggerwareClientException if this QueryStatement is closed
	 */
	public <T> TWResultSet<T> executeQuery(Class<T>rowClass, String query,  String schema, QueryResourceLimits qrl)
				throws JRPCException, TriggerwareClientException{
		commonCheck();
		var jt =  typeFactory.constructParametricType(QueryStatement.ResultSetResult.class, rowClass);
		var eqNPR = new AdHocQueryRequest<T>(jt, rowClass);
		var params = commonParams(query, schema);
		if (qrl!=null) params.with("limit", qrl.rowCountLimit).with("timelimit", qrl.timeout);
		var eqresult = (ResultSetResult<T>)connection.synchronousRPC(eqNPR, params);
		eqresult.setRowConstructor(eqNPR.rowConstructor);
		var rs = new TWResultSet<T>(eqNPR.rowConstructor, connection, eqresult.handle, fetchSize, eqresult.rowSignature, eqresult.batch.getRows());
		eqresult.setResultSet(rs);
		resultSet = rs;
		return rs;
	}
	
	/**
	 * execute an ad-hoc SQL query, returning a resultset. The fetchsize of this QueryStatement is use as a rowCountLimit.
	 * @param <T> the row type for the results
	 * @param rowClass the row type for a row of results from this query
	 * @param query  the query string
	 * @param schema the schema name to use for interpreting the query string
	 * @return a TWResultSet with the first batch of results from executing this ad hoc query
	 * @throws JRPCException a variety of problems -- communications, protocol, serialization/deserialization, or method-specific
	 * @throws TriggerwareClientException if this QueryStatement is closed 
	 */
	public  <T> TWResultSet<T> executeQuery(Class<T> rowClass, String query,  String schema)
			throws JRPCException, TriggerwareClientException{
		var qrl = fetchSize==null? null : new QueryResourceLimits().withLimit(fetchSize);
		return executeQuery(rowClass, query, schema, qrl);	
	}
	
	/**
	 * execute an ad-hoc query, returning a resultset. The fetchsize of this querystatement is use as a rowCountLimit.
	 * @param <T> the row type for the results
	 * @param rowClass the row type for a row of results from this query
	 * @param query  the query string
	 * @param schema the schema  name to use for interpreting the query string
	 * @param queryLanguage -- either Language.FOL or Language.SQL, as appropriate for the query
	 * @return a TWResultSet with the first batch of results from executing this ad hoc query
	 * @throws JRPCException a variety of problems -- communications, protocol, serialization/deserialization, or method-specific
	 * @throws TriggerwareClientException if this QueryStatement is closed 
	 */
	/*<T> TWResultSet<T> executeQuery(Class<T> rowClass, String query,  String schema, String queryLanguage)
			throws JRPCException, TriggerwareClientException{
		var qrl = fetchSize==null? null : new QueryResourceLimits().withLimit(fetchSize);
		return executeQuery(rowClass, query, schema, qrl);	
	}*/

	/*
	- method - method to use in notifications
	 - notify-limit - this many rows waiting to be reported   causes notification
	 - notify-timelimit - a row waiting to be reported for this long causes notification 
	   (number of seconds, needn't be integral -- or null for no limit on time)
	 */
	/**	 * 
	 * @param <T> the row type for the results
	 * @param rowClass the class of the row type
	 * @param query  the query string
	 * @param schema the schema name to use for interpreting the query string
	 * @param controller controls that affect the timing/batching of results
	 * @throws JRPCException a variety of problems -- communications, protocol, serialization/deserialization, or method-specific
	 * @throws TriggerwareClientException if this QueryStatement is closed or if it is still executing some other query
	 */
	@SuppressWarnings("unchecked")
	<T> void executeQueryWithNotificationResults(Class<T>rowClass, String query,  String schema,
						NotificationResultController<T> controller) throws TriggerwareClientException, JRPCException{
		commonCheck();
		if (controller.getHandle() != null)
			throw  new TriggerwareClientException("using an open NotificationResultController with a new query.");
		var jt = typeFactory.constructParametricType(ResultSetResult.class, rowClass);
		var eqParams = commonParams(query,schema).with("limit", 0);
		Constructor<T> rowConstructor = AbstractQuery.getRowConstructor(rowClass);
		var eqresult = (ResultSetResult<T>)connection.synchronousRPC(jt, /*null,*/  "execute-query", eqParams);//just get the resultset, but no rows
		controller.setHandle(connection, eqresult.handle, eqresult.getSignature(), rowConstructor, eqresult.getResultSet()); //that registers the handler
		//now start streaming
		var nriParams = controller.controlParams();
		connection.synchronousRPC(Void.TYPE, /*null,*/ "next-resultset-incremental", nriParams); //tell server to start streaming
	}

	private boolean closed = false;
	@Override
	public void close()  {
		if (closed) return;
		if (resultSet != null) {
			try {
				resultSet.close();
			}catch(Throwable t) {}
			resultSet = null;
		}
		closed = true;
	}
	@Override
	public Connection getConnection() {	return connection;	}
}
