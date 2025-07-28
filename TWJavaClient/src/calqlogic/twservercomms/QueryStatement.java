package calqlogic.twservercomms;

import java.util.ArrayList;


import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.type.TypeFactory;

import calqlogic.twservercomms.AbstractQuery.SignatureElement;
import calqlogic.twservercomms.TriggerwareClient.TriggerwareClientException;
import nmg.softwareworks.jrpcagent.*;

/**
 * A statement than can be used to issue query requests on a connection.
 * The statement may be executed {link #executeQuery} multiple times on the connection.
 * A QueryStatement may also be executed using {link #executeQueryWithPushResults} to have the rows 'streamed' back to
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
	QueryStatement(TriggerwareClient client) {
		this(client.getPrimaryConnection());}

	/**
	 * create a new query statement
	 * @param connection the connection on which the query will be issued.
	 */
	QueryStatement(TriggerwareConnection connection) {
		this.connection = connection;
		this.typeFactory = connection.getTypeFactory();
		this.fetchSize = connection.getAgent().getDefaultFetchSize();
	}

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
	static class Batch<T>{
		private final long count;
		private final ArrayList<T> rows;
		private final boolean exhausted;
		@JsonCreator 
		public Batch(@JsonProperty("count") int count, @JsonProperty("tuples") ArrayList<T> rows,
						@JsonProperty("exhausted")Boolean exhausted){
			 this.count = count;
			 this.rows = rows;
			 if (exhausted != null)
				 this.exhausted = exhausted;
			 else this.exhausted = false;
		 }
		public long getCount() {return count;}
		public ArrayList<T>getRows(){return rows;}
		public boolean isExhausted() {return exhausted;}
	}
	@JsonFormat(shape=JsonFormat.Shape.OBJECT)
	static class ExecuteQueryResult<T> {
		private final Integer handle;
		private final Batch<T> batch;
		private final SignatureElement[]signature;
		Integer getHandle() {return handle;}
		Batch<T>getBatch(){return batch;}
		@JsonCreator 
		public ExecuteQueryResult(@JsonProperty("handle") Integer handle, @JsonProperty("batch") Batch<T> batch,
						@JsonProperty("signature")SignatureElement[]signature){
			 this.handle = handle;
			 this.batch = batch;
			 this.signature = signature;
		 }
		public SignatureElement[]getSignature(){
			return signature;}
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
	 * by the QueryResourceLimits.  Even if a query executed by executeQuery has its own limit clause, the rowCountLimit
	 * can further limit the number of 'row' values that will be returned. It cannot <em>extend</em> that limit, however.</li>
	 * </ul>
	 */
	public static class QueryResourceLimits{
		private Integer rowCountLimit = null;//, notifyRowCountLimit = null;
		private Double timeout = null;//, notifyTimeout = null;
		/**
		 //private  Class<?>[]  signatureTypes;  //private  String[] signatureNames, signatureTypeNames;
		 * create a new QueryResourceLimits with default limits.
		 * No timeout, and no limit on the number of row values to be returned.
		 */
		public QueryResourceLimits() {}
		
		/**
		 * create a new QueryResourceLimits with specific limits.
		 * @param timeout a timeout value
		 * @param rowCountLimit  a limit on the number of row values to be returned
		 */
		public QueryResourceLimits(Double timeout, Integer rowCountLimit) {
			setTimeout(timeout);
			setRowCountLimit(rowCountLimit);
		}

		public Integer getRowCountLimit() {return rowCountLimit;}
		public void setRowCountLimit(Integer limit) {
			if (limit != null && limit < 1)
			  throw new IllegalArgumentException(
					  "limit value passed to QueryResourceLimits.setRowCountLimit must be null or positive");		
			rowCountLimit = limit;}
		public Double getTimeout() {return timeout;}
		/**
		 * set the timeout resource limit
		 * @param timeout null for no timeout, or a positive value.
		 */
		public void setTimeout(Double timeout) {
			if (timeout != null && timeout <= 0.0)
				throw new IllegalArgumentException(
						"timeout value passed to QueryResourceLimits.setTimeout must be null or positive");
			this.timeout = timeout;}
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
	public  TWResultSet<Object[]> executeQuery(String query,  String schema)
			throws JRPCException, TriggerwareClientException{
		var qrl = fetchSize==null? null : new QueryResourceLimits(null, fetchSize);
		return executeQuery(Object[].class, query, Language.SQL, schema, qrl);	
	}
	
	/**
	 * execute an ad-hoc SQL query, returning a resultset
	 * No resource usage limits are imposed;all rows are returned in  the first resultset batch.
	 * @param query  the SQL query string
	 * @param schema the schema to use for resolving table and function names in the query string
	 * @return a TWResultSet with the first batch of results from executing this PreparedQuery
	 * @throws JRPCException a variety of problems -- communications, protocol, serialization/deserialization, or method-specific
	 * @throws TriggerwareClientException if this QueryStatement is closed 
	 */
	public <T> TWResultSet<T> executeQuery(Class<T> rowType, String query,  String schema)
			throws JRPCException, TriggerwareClientException{
		var qrl = fetchSize==null? null : new QueryResourceLimits(null, fetchSize);
		return executeQuery(rowType, query, Language.SQL, schema, qrl);	
	}

	/**
	 * execute an ad-hoc query, returning a resultset
	 * Object[] means that each row element in a result is deserialized from json using the default interpretation
	 * as a Java object. The final argument null means that no resource usage limits are imposed, and all rows are returned in 
	 * the first resultset batch.
	 * @param query  the query string
	 * @param schema the schema (sql) or package (fol) name to use for interpreting the query string
	 * @param queryLanguage -- either Language.FOL or Language.SQL, as appropriate for the query
	 * @return a TWResultSet with the first batch of results from executing this PreparedQuery
	 * @throws JRPCException a variety of problems -- communications, protocol, serialization/deserialization, or method-specific
	 * @throws TriggerwareClientException if this QueryStatement is closed 
	 */
	public  TWResultSet<Object[]> executeQuery(String query,  String queryLanguage, String schema)
			throws JRPCException, TriggerwareClientException{
		var qrl = fetchSize==null? null : new QueryResourceLimits(null, fetchSize);
		return executeQuery(Object[].class, query, queryLanguage, schema, qrl);	
	}
	
	private void commonCheck() throws TriggerwareClientException {
		if (closed) throw  new TriggerwareClientException("attempt to execute a closed QueryStatement");
		if (resultSet != null) {
			resultSet.close();
			resultSet = null;
		}		
	}
	
	private NamedRequestParameters commonParams(String query, String queryLanguage, String schema) {
		return new NamedRequestParameters().with("query", query).with("language", queryLanguage).with("namespace", schema)
				.with("check-update", false);
	}

	/**
	 * @param <T> the row type for the results
	 * @param rowType the class of the row type
	 * @param query  the query string
	 * @param queryLanguage  either Language.FOL or Language.SQL, as appropriate for the query
	 * @param schema the schema (sql) or package (fol) name to use for interpreting the query string
	 * @param qrl resource limits on executing the query. null means no limits.
	 * @return a TWResultSet with the first batch of results from executing this query
	 * @throws JRPCException a variety of problems -- communications, protocol, serialization/deserialization, or method-specific
	 * @throws TriggerwareClientException if this QueryStatement is closed
	 */
	@SuppressWarnings("unchecked")
	public <T> TWResultSet<T> executeQuery(Class<T>rowType, String query, String queryLanguage, String schema, 
			QueryResourceLimits qrl)	throws JRPCException, TriggerwareClientException{
		commonCheck();
		var jt = typeFactory.constructParametricType(QueryStatement.ExecuteQueryResult.class, rowType);
		var params = commonParams(query, queryLanguage, schema);
		if (qrl!=null) {
			params.with("limit", qrl.rowCountLimit).with("timelimit", qrl.timeout);
		} //else params.with("limit", Short.MAX_VALUE).with("timelimit", null);
		ExecuteQueryResult<T> eqresult = null;
		/*if (qrl != null && qrl.timeout != null) { //this is for the case that we aren't looking for 'partial success' from a request
			var future = connection.asynchronousRPCN(jt, null, true, "execute-query",  params);
			try {
				eqresult = (ExecuteQueryResult<T>)JRPCAsyncRequest.executeWithTimeout(future, (long)(qrl.timeout*1000));
			} catch (TimeoutException e) {
				future.cancel(false); //that will cause the agent to ignore the eventual result.
				throw new TimeoutException("client timeout  waiting for executeQuery result");
			}catch (ExecutionException e) { // the request terminated by throwing an exception
				var cause = e.getCause();
				if (cause instanceof JRPCException) throw (JRPCException)cause;
				if (cause instanceof TriggerwareClientException) throw (TriggerwareClientException)cause; // is that possible? maybe from deserializing
				throw new JRPCException.InternalJRPCException(cause);
			}catch(CancellationException e) {//this is a runtime exception in java
				throw e;
			}catch (InterruptedException  e) {
				throw new JRPCException.InterruptionError(e);
			}
		}
		else */
			eqresult = connection.synchronousRPC(jt, null, null, "execute-query", params);
		var rs = new TWResultSet<T>(rowType, this, fetchSize, eqresult,null);
		resultSet = rs;
		return rs;
	}
	
	/*
	- method - method to use in notifications
	 - notify-limit - this many rows waiting to be reported   causes notification
	 - notify-timelimit - a row waiting to be reported for this long causes notification 
	   (number of seconds, needn't be integral -- or null for no limit on time)
	 */
	/**	 * 
	 * @param <T> the row type for the results
	 * @param rowType the class of the row type
	 * @param query  the query string
	 * @param queryLanguage  either Language.FOL or Language.SQL, as appropriate for the query
	 * @param schema the schema (sql) or package (fol) name to use for interpreting the query string
	 * @param controller controls that affect the timing/batching of results
	 * @throws JRPCException a variety of problems -- communications, protocol, serialization/deserialization, or method-specific
	 * @throws TriggerwareClientException if this QueryStatement is closed or if it is still executing some other query
	 */
	@SuppressWarnings("unchecked")
	public <T> void executeQueryWithPushResults(Class<T>rowType, String query, String queryLanguage, String schema,
												PushResultController<T> controller) 
					throws TriggerwareClientException, JRPCException{
		commonCheck();
		if (controller.getHandle()!=null)
			throw  new TriggerwareClientException("using an open PushResultController with a new query.");
		var jt = typeFactory.constructParametricType(QueryStatement.ExecuteQueryResult.class, rowType);
		var eqParams = commonParams(query, queryLanguage, schema).with("limit", 0);
		ExecuteQueryResult<T> eqresult = connection.synchronousRPC(jt, null, null,  "execute-query", eqParams);
		controller.setHandle(connection, eqresult.handle); //that registers the handler
		var nriParams = controller.getParams();
		//now start streaming
		connection.synchronousRPC(Void.TYPE, null, null, "next-resultset-incremental", nriParams); //tell server to start streaming
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
