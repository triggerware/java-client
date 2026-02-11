package calqlogic.twservercomms;

import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.*;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;

import calqlogic.twservercomms.AbstractQuery.SignatureElement;
import calqlogic.twservercomms.TriggerwareClient.TriggerwareClientException;
import nmg.softwareworks.jrpcagent.*;
import nmg.softwareworks.jrpcagent.JRPCException.JRPCRequestTimeoutException;

/**
 * <p>a TWResultSet represents the set of rows that are the result of a query.  A TWResultSet provides
 * forward-only iteration over that set of rows via {@link #next()} and {@link #get()}.  
 * At any time a TWResultSet  holds a cache of rows received
 * from the server but not yet delivered to the iteration.  An attempt to iterate to the next
 * rpw when the cache is empty will cause the TWResultSet to request more rows from the server.
 * </p><p>
 * Additonal rows can be requested from the server explicitly using the method {@link #augmentCache()}.
 * </p><p>
 * Each row of a TWResultSet set is held by a Java value of type T, which is itself a type parameter of the query that produced the TWResultSet.
 * If T is an array type (e.g., Object[]) or a list collection type (e.g., ArrayList&lt;Object&gt;),
 * then each row will have the same length, and will
 * have elements belonging to the types of the TWResultSet's signature.  The length and type restriction
 * are <em>ensured</em> correct when the rows are produced (by deserializing them from results of TW server requests.)
 *</p><p>
 *It is generally useful for applications to define a java class (POJO) to represent a row of a query result, and to use
 *that type as the row type T.  The Java class needs to have a field of the proper type for each column of the row, and a
 *public constructor (for use in deserializing the TWResultSet value) with parameters of the type and order of the columns.
 * </p><p>
 * This implementation of TWResultSet is thread safe.
 * </p>
 *
 * @param <T> the type that will hold a row of results
 */
public class TWResultSet<T> implements  AutoCloseable{
	
	/**
	 * a TWResultSetException can be thrown from a number of methods due to improper use of a TWResultSet
	 * Those methods document the cases where such an exception can occur.
	 *
	 */
	public static class TWResultSetException extends TriggerwareClientException{
		private TWResultSetException(String message) {
			super(message);
		}
		private TWResultSetException(String message, Exception e) {
			super(message,e);
		}
	}
	private static final TWResultSetException closedResultSetException =
			new TWResultSetException("operation on a closed ResultSet");
 

	/**
	 * the constructor (if any) for a row of the results
	 */
	private final Constructor<T>rowConstructor;
	/**
	 * signature is the actual query result signature returned from the query translator (in the case of an SQL query)
	 * In the case of an FOL query, there is still a signature but I do not know where the types come from.
	 */
	private final SignatureElement[]signature;
	private final String[]columnNames, columnTypes;

	private boolean closed = false; // closed means that no more requests involving this resultset are permissible
	private boolean exhausted; // exhausted means that the TW server will not supply any further rows
	private final LinkedList<T> cache = new LinkedList<T>();
	//private final Statement statement;
	private int cacheSize = 0;
	private boolean isBeforeFirst = true;
	private boolean isAfterLast = false;
	private Object current = null;
	private Integer fetchSize; //null means as many as possible
	private Double timeout = null; //null means no timeout
	@JsonProperty("handle")
	private final Integer handle;
	@JsonProperty("batch")
	private Batch<T> batch;
	private final Connection connection;
	private final Integer maxRows;
	private int rowsSoFar;
	private final JavaType jtForNextBatchRequest;
	TWResultSet(Constructor<T>rowConstructor, Connection connection, Integer handle, Integer fetchSize,  Integer maxRows, SignatureElement[]signature, ArrayList<T>rows){
		this.rowConstructor = rowConstructor;
		this.connection = connection;
		this.fetchSize = fetchSize;
		this.maxRows = maxRows;
		this.handle = handle;
		exhausted = (handle == null);
		jtForNextBatchRequest =  connection.getTypeFactory().constructType(Batch.class);
		if (rows == null)	cacheSize = 0;
		else {
			cacheSize = rows.size();
			cache.addAll(rows);
		}

		rowsSoFar = cacheSize;
		this.signature = signature;//eqResult.getSignature();
		if (signature != null) {
			columnNames = new String[signature.length];
			columnTypes = new String[signature.length];
			for (int i=0; i<columnNames.length; i++) {
				columnNames[i] = signature[i].name;
				columnTypes[i] = signature[i].typeName;
			}
		} else columnNames = columnTypes = null;
	}

	/**
	 * Obtain the signature of this TWResultSet. Each row delivered by {@link #get()} is an Object[] whose
	 * length is the signature's length and whose ith element is an instance of the class that is the ith element
	 * of the signature. Applications will rarely need this method because the programmer will know at coding time
	 * what the row element types are, and will simply cast the element values to the promised type when consuming
	 * them.
	 * @return the signature of the rows delivered by this TWResultSet. 
	 */
	public SignatureElement[] getRowSignature() {return signature;	}
	
	int getHandle() {return handle;}

	/**
	 * @return the Connection on which this TWResultSet was created. 
	 */
	public Connection getConnection() {return connection;}
	
	/**
	 * @return the names of the columns for the columns of this resultset
	 */
	public String[] getColumnNames() {return columnNames;}
	
	public String[] getColumnTypeNames(){return columnTypes;}
	
	/**
	 * @return the statement used to create this TWResultSet
	 */
	//public Statement getStatement() {return statement;}

	/**
	 * Change the fetch size to use if {@link #next()} needs to retrieve additional rows from the TW Server
	 * @param size The fetch size to use
	 */
	synchronized public void setFetchSize(Integer size) {fetchSize = size;}
	/**
	 * @return The fetch size that will be used if {@link #next()} needs to retrieve additional rows from the TW Server
	 */
	synchronized public Integer getFetchSize() {return fetchSize;}

	/**
	 *  Change the timeout to use if {@link #next()} needs to retrieve additional rows from the TW Server
	 * @param timeout The timeout to use, measure in seconds
	 */
	synchronized public void setTimeout(Double timeout) {this.timeout = timeout;}
	/**
	 * @return The timeout that will be used if {@link #next()} needs to retrieve additional rows from the TW Server,
	 * measure in seconds
	 */
	synchronized public Double getTimeout() {return timeout;}

	/**
	 * @return <code>true</code> if {@link #next()} has not been invoked yet for this TWResultSet. Otherwise <code>false</code> 
	 */
	synchronized public boolean isBeforeFirst() {return isBeforeFirst;}
	/**
	 * @return <code>true</code> if {@link #next()} has been invoked for this TWResultSet and returned <code>false</code>,
	 * indicating that no more rows are available. Otherwise <code>false</code>
	 */
	synchronized public boolean isAfterLast() {return isAfterLast;}
	/**
	 * @return <code>true</code> if this TWResultSet has been closed. Otherwise <code>false</code>.
	 * A TWResultSet can be explicitly closed by the {@link #close} method, or indirectly closed if
	 * the statement which produced it is closed.	 * 
	 */
	synchronized public boolean isClosed() {return closed;}

	/**
	 * @return <code>true</code> if this TWResultSet can obtain no more rows from the TW server. Otherwise <code>false</code>.
	 */
	synchronized public boolean isExhausted() {return exhausted;}
	
	//synchronized int getRow() {return rowNumber;}
	/**
	 * @return the number of rows remaining in the cache. Calling {@link #next()} more
	 * than this number of times would cause this resultset to request more rows from the server.
	 */
	synchronized public int getCacheSize() {return cacheSize;}
	/**
	 * @param clearCache if true, the cache is cleared. Attempts to retrieve more elements will fail (if the resultset was
	 * exhausted) or will try to retrieve the next batch from the server.
	 * @return a list of the rows remaining in the cache (before clearing it).  
	 * This does <em>not</em> include the current row (the one that would be returned by <em>get()</em>
	 */
	synchronized public ArrayList<T> cacheSnapshot(boolean clearCache) {
		var snap = new ArrayList<T>(cacheSize);
		snap.addAll(cache);
		if (clearCache) {
			cache.clear();
			cacheSize = 0;
		}
		return snap;
	}
	/**
	 * determine if another row can be obtained from this TWResultSet.  This may require
	 * a request for more rows to be issued to the TW server. 
	 * @return <code>true</code> if any more rows remain, otherwise <code>false</code>
	 * When a row is available, it is made the current row and can be obtained via {@link #get()}
	 * @throws TWResultSetException if this TWResultSet has been closed and the cache is empty
	 */
	synchronized public boolean next() throws TWResultSetException {
		//if (closed) throw closedResultSetException;
		if (!cache.isEmpty()) {
			isBeforeFirst = false;
			current = cache.removeFirst();
			//rowNumber++;
			cacheSize--;
			return true;
		}
		if (exhausted) {
			isAfterLast = true;
			return false;
		}
		return augmentCache();
	}
	
	/**
	 * request another batch of rows for this resultset from the tw server.  The request uses this resultset's current fetchsize and timeout.
	 * Use this method to obtain more rows even if the current cache of rows may not have all been consumed.
	 * @return true if one or more additional rows were received, false if no more rows are available.
	 * @throws TWResultSetException if this TWResultSet has been closed
	 */
	synchronized public boolean augmentCache() throws TWResultSetException{
		if (closed) throw closedResultSetException;
		// ask TW server for another batch
		Batch<T> nextBatch;
		try {
			 nextBatch = pulse();
		}catch (JRPCRequestTimeoutException e) {
			close();
			throw new TWResultSetException("agent abandoned resultset", e);
		} catch(JRPCException e) {
			close();
			throw new TWResultSetException("internal error", e);
		}
		var rows = nextBatch == null ? null : nextBatch.getRows();
		if (rows==null || rows.isEmpty()) {
			//rowNumber = 0;
			isAfterLast = true; 
			//conceivably you could have a fetchsize of zero? So no rows, but not exhausted? Currently tw server requires it to be positive
			exhausted = nextBatch == null || nextBatch.isExhausted();
			closed = exhausted;
			return false;
		} else {
			cache.addAll(rows);
			rowsSoFar += rows.size();
			cacheSize += rows.size();
			exhausted = nextBatch.isExhausted();
			isBeforeFirst = false;
			current = cache.removeFirst();
			//rowNumber++;
			cacheSize--;
			return true;
		}		
	}
	
	/**
	 * @return the current row
	 * @throws TWResultSetException if this TWResultSet is closed, or if {@link #next()} has not yet been called
	 * or has already returned false. 
	 */
	synchronized public Object get() throws TWResultSetException  {
		if (closed) throw closedResultSetException;
		if (isBeforeFirst) throw new TWResultSetException("get called before next()");
		if (isAfterLast) throw new TWResultSetException("get called after next() returned false");
		return current;
	}
	private static class NextResultSetRequest<T>extends PositionalParameterRequest<T>{//T is the java static type for a row
		private final Constructor<T>rowConstructor;
		private final SignatureElement[]sig;
		NextResultSetRequest(JavaType responseType,  Constructor<T>rowConstructor, SignatureElement[]sig){
			super(responseType,  "next-resultset-batch",  1, 3); //args are handle of resultset, countlimit, timelimit
			this.rowConstructor = rowConstructor;
			this.sig = sig;
		}
		/**
		 * establishResponseDeserializationAttributes is used internally to establish context used to deserialize JRPC messages. It is not intended to be overridden or called in application code.
		 * @param request
		 * @param response
		 * @param ctxt
		 */
		@Override
		public void establishResponseDeserializationAttributes(JRPCSimpleRequest<?> request, IncomingMessage response, DeserializationContext ctxt) {
			@SuppressWarnings("unchecked")
			var dsstate= (HashMap<String,Object>)ctxt.getAttribute("deserializationState");
			dsstate.put("rowSignature", sig);
			if (rowConstructor != null) 
				dsstate.put("rowBeanConstructor", rowConstructor);
		}
	}
	/*@SuppressWarnings("rawtypes")
	private static PositionalParameterRequest<TWResultSet.Batch> nextBatchRequest = 
			new PositionalParameterRequest<TWResultSet.Batch>(TWResultSet.Batch.class, null, "next-resultset-batch", 2, 3);*/
	@SuppressWarnings("unchecked")
	private Batch<T> pulse() throws JRPCException {
		//TODO: is it possible to make this request deserialize its result into the existing resultset object?

		var nrsPPR = new NextResultSetRequest<T>(jtForNextBatchRequest, rowConstructor, signature);
		Integer pulseFetchSize = (maxRows == null) ? fetchSize : Integer.min(fetchSize, maxRows-rowsSoFar);
		if (pulseFetchSize == 0) {
				this.close(); // don't ask for more,just close the resultset
				return null;
		}
		if (timeout==null) 
			return(Batch<T>)connection.synchronousRPC(nrsPPR, new Object[] {handle, pulseFetchSize});
			
		var future = connection.asynchronousRPC(nrsPPR, new Object[] {handle, pulseFetchSize, timeout});
		try {
			return (Batch<T>)JRPCAsyncRequest.executeWithTimeout(nrsPPR, future, (long)(timeout*1000));
		} catch (JRPCRequestTimeoutException e) {
			future.cancel(false); //that will cause the agent to ignore the eventual result.
			throw e;
		}catch(CancellationException e) {//this is a runtime exception in java
			throw e;
		}catch (InterruptedException | ExecutionException  e) {
			return null;
		}
	}

	static PositionalParameterRequest<Void> closeResultSetRequest = 
			new PositionalParameterRequest<Void>(Void.TYPE, "close-resultset", 1, 1);
	/**
	 * close this TWResultSet explicitly.  If necessary, this method will notify the TWServer to release
	 * resources being preserved to produce further rows.
	 */
	@Override
	public synchronized void close() {
		if (closed) return;
/*  */
		if (exhausted) { // no need to tell TW to release it
			closed = true;
			return;
		}
		exhausted = true;
		closed = true;
		try {closeResultSetRequest.execute(connection, handle);
		}catch(JRPCException e) {
			//Logging.log("error closing a TWResultSet <%s>",e.getMessage());
		}
	}
}
