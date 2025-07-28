package calqlogic.twservercomms;

import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import calqlogic.twservercomms.AbstractQuery.SignatureElement;
import calqlogic.twservercomms.TriggerwareClient.TriggerwareClientException;
import nmg.softwareworks.jrpcagent.*;

/**
 * <p>a TWResultSet represents the set of rows that are the result of a query.  It provides
 * forward-only iteration over that set via {@link #next()} and {@link #get()}.  
 * At any time a TWResultSet  holds a cache of rows received
 * from the server but not yet delivered to the iteration.  An attempt to iterate to the next
 * rpw when the cache is empty will cause the TWResultSet to request more rows from the server.
 * </p><p>
 * Each row of a result set is Java Object[] value.  Each row will have the same length, and will
 * have elements belonging to the types of the TWResultSet's signature.  The length and type restriction
 * are <em>ensured</em> correct when the rows are produced (by deserializing them from results of TW server requests.)
 * However, because of limitations of Java's generic type capability it is not possible to have a proper generic
 * row type for each TWResultSet.
 * </p><p>
 * This implementation of TWResultSet is <em>not</em> thread safe.  A program that accesses a
 * TWResultset from multiple threads is responsible for serializing access to it.
 * </p>
 * @author nmg
 *
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
 
	private final Class<T> rowType;
	private final SignatureElement[]signature;
	private final String[]columnNames;
	@SuppressWarnings("unused")
	private final PreparedQuery<T> fromQuery ;
	private boolean closed = false; // closed means that the consumer of this resultset will ask for no more rows
	private boolean exhausted; // exhausted means that the TW server will not supply any further rows
	private final LinkedList<T> cache = new LinkedList<T>();
	private final Statement statement;
	private int cacheSize = 0;
	private boolean isBeforeFirst = true;
	private boolean isAfterLast = false;
	private T current = null;
	private int rowNumber = 0;
	private Integer fetchSize; //null means as many as possible
	private Double timeout = null; //null means no timeout
	@JsonProperty("handle")
	Integer handle;
	@JsonProperty("batch")
	private Batch<T> batch;
	private final Connection connection;
	TWResultSet(Class<T>rowType, Statement statement, Integer fetchSize,  QueryStatement.ExecuteQueryResult<T>eqResult,
			PreparedQuery<T> pq){
		this.rowType = rowType;
		this.statement = statement;
		this.connection = statement.getConnection();
		this.fetchSize = fetchSize;
		handle = eqResult.getHandle();
		exhausted = (handle == null);
		var rows = eqResult.getBatch().getRows();
		if (rows == null)	cacheSize = 0;
		else {
			cacheSize = rows.size();
			cache.addAll(rows);
		}
		if (pq == null) {
			fromQuery = null;
			signature = eqResult.getSignature();
			if (signature != null) {
				columnNames = new String[signature.length];
				for (int i=0; i<columnNames.length; i++)
					columnNames[i] = signature[i].name;
			} else columnNames = null;
		} else {
			signature = null;
			fromQuery = pq;
			columnNames = pq.getInputNames();
		}
			
		
	}

	/**
	 * Obtain the signature of this TWResultSet. Each row delivered by {@link #get()} is an Object[] whose
	 * length is the signature's length and whose ith element is an instance of the class that is the ith element
	 * of the signature. Applications will rarely need this method because the programmer will know at coding time
	 * what the row element types are, and will simply cast the element values to the promised type when consuming
	 * them.
	 * @return the signature of the rows delivered by this TWResultSet. 
	 */
	public Class<T> getRowSignature() {return rowType;	}
	
	int getHandle() {return handle;}

	/**
	 * @return the Connection on which this TWResultSet was created. 
	 */
	public Connection getConnection() {return connection;}
	
	/**
	 * @return the names of the columns for the columns of this resultset
	 */
	public String[] getColumnNames() {return columnNames;}
	
	/**
	 * @return the statement used to create this TWResultSet
	 */
	public Statement getStatement() {return statement;}

	/**
	 * Change the fetch size to use if {@link #next()} needs to retrieve additional rows from the TW Server
	 * @param size The fetch size to use
	 */
	public void setFetchSize(Integer size) {fetchSize = size;}
	/**
	 * @return The fetch size that will be used if {@link #next()} needs to retrieve additional rows from the TW Server
	 */
	public Integer getFetchSize() {return fetchSize;}

	/**
	 *  Change the timeout to use if {@link #next()} needs to retrieve additional rows from the TW Server
	 * @param timeout The timeout to use, measure in seconds
	 */
	public void setTimeout(Double timeout) {this.timeout = timeout;}
	/**
	 * @return The timeout that will be used if {@link #next()} needs to retrieve additional rows from the TW Server,
	 * measure in seconds
	 */
	public Double getTimeout() {return timeout;}

	/**
	 * @return <code>true</code> if {@link #next()} has not been invoked yet for this TWResultSet. Otherwise <code>false</code> 
	 */
	public boolean isBeforeFirst() {return isBeforeFirst;}
	/**
	 * @return <code>true</code> if {@link #next()} has been invoked for this TWResultSet and returned <code>false</code>,
	 * indicating that no more rows are available. Otherwise <code>false</code>
	 */
	public boolean isAfterLast() {return isAfterLast;}
	/**
	 * @return <code>true</code> if this TWResultSet has been closed. Otherwise <code>false</code>.
	 * A TWResultSet can be explicitly closed by the {@link #close} method, or indirectly closed if
	 * the statement which produced it is closed.	 * 
	 */
	public boolean isClosed() {return closed;}

	/**
	 * @return <code>true</code> if this TWResultSet can obtain no more rows from the TW server. Otherwise <code>false</code>.
	 */
	public boolean isExhausted() {return exhausted;}
	
	int getRow() {return rowNumber;}
	/**
	 * @return the number of rows remaining in the cache. Calling {@link #next()} more
	 * than this number of times would cause this resultset to request more rows from the server.
	 */
	public int getCacheSize() {return cacheSize;}
	/**
	 * @param clearCache if true, the cache is cleared. Attempts to retrieve more elements will fail (if the resultset was
	 * exhausted) or will try to retrieve the next batch from the server.
	 * @return a list of the rows remaining in the cache (before clearing it).  
	 * This does <em>not</em> include the current row (the one that would be returned by <em>get()</em>
	 */
	public ArrayList<T> cacheSnapshot(boolean clearCache) {
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
	 * @throws TWResultSetException if this TWResultSet has been closed
	 * @throws TimeoutException if more row need to be obtained from the TW server and the request times out
	 */
	public synchronized boolean next() throws TWResultSetException, TimeoutException {
		if (closed) throw closedResultSetException;
		if (!cache.isEmpty()) {
			isBeforeFirst = false;
			current = cache.removeFirst();
			rowNumber++;
			cacheSize--;
			return true;
		}
		if (exhausted) {
			isAfterLast = true;
			return false;
		}
		// ask TW for another batch
		Batch<T> nextBatch;
		try {
			 nextBatch = pulse();			
		} catch(JRPCException e) {
			close();
			throw new TWResultSetException("internal error", e);
		}
		var rows = nextBatch.rows;
		if (rows==null || rows.isEmpty()) {
			rowNumber = 0;
			isAfterLast = true;
			exhausted = true;
			closed = true;
			return false;
		} else {
			cache.addAll(rows);
			cacheSize += rows.size();
			exhausted = nextBatch.exhausted;
			isBeforeFirst = false;
			current = cache.removeFirst();
			rowNumber++;
			cacheSize--;
			return true;
		}
	}
	
	/**
	 * @return the current row
	 * @throws TWResultSetException if this TWResultSet is closed, or if {@link #next()} has not yet been called
	 * or has already returned false. 
	 */
	public T get() throws TWResultSetException  {
		if (closed) throw closedResultSetException;
		if (isBeforeFirst) throw new TWResultSetException("get called before next()");
		if (isAfterLast) throw new TWResultSetException("get called after next() returned false");
		return current;
	}

	@SuppressWarnings("rawtypes")
	private static final PositionalParameterRequest<TWResultSet.Batch> nextBatchRequest = 
			new PositionalParameterRequest<TWResultSet.Batch>(TWResultSet.Batch.class, null, "next-resultset-batch", 2, 3);
	@SuppressWarnings("unchecked")
	private Batch<T> pulse() throws JRPCException, TimeoutException {
		//TODO: make this request deserialize its result into the existing resultset object?
		if (timeout==null) return nextBatchRequest.execute(connection, handle, fetchSize);
			
		var future = connection.asynchronousRPC(TWResultSet.Batch.class, null, TriggerwareClient.serverAsynchronousMap, "next-resultset-batch",  
				handle, fetchSize, timeout);
		try {
			return (Batch<T>)JRPCAsyncRequest.executeWithTimeout(future, (long)(timeout*1000));
		} catch (TimeoutException e) {
			future.cancel(false); //that will cause the agent to ignore the eventual result.
			throw new TimeoutException("client timeout waiting for next batch of results");
		}catch(CancellationException e) {//this is a runtime exception in java
			throw e;
		}catch (InterruptedException | ExecutionException  e) {
			return null;
		}
	}

	static PositionalParameterRequest<Void> closeResultSetRequest = 
			new PositionalParameterRequest<Void>(Void.TYPE, null, "close-resultset", 1, 1);
	/**
	 * close this TWResultSet explicitly.  If necessary, this method will notify the TWServer to release
	 * resources being preserved to produce further rows.
	 */
	@Override
	public synchronized void close() {
		if (closed) return;
		rowNumber = 0;
		if (exhausted) { // no need to tell TW to release it
			closed = true;
			return;
		}
		exhausted = true;
		closed = true;
		try {closeResultSetRequest.execute(connection, handle);
		}catch(JRPCException e) {
			Logging.log("error closing a TWResultSet <%s>",e.getMessage());
		}
	}
	
	@JsonIgnoreProperties(ignoreUnknown = true) //a count attribute is included in the serialization from tw
	static class Batch<T>{
		/*@JsonProperty("count") //needed if field is private and has no getter
		private int count;*/
		@JsonProperty("exhausted")
		private boolean exhausted = false;
		@JsonProperty("rows")
		private ArrayList<T> rows;
		Batch() {} //for default jackson deserializer
		/*Batch(Class<?>[] signature){this.signature = signature;}
		
		private static Object[] tupleFromJson(ArrayNode jtuple, Class<?>[]sig) {
			var tuple = new Object[jtuple.size()];
			for (int i=0; i<jtuple.size(); i++) {
				tuple[i] = JsonUtilities.deserialize(jtuple.get(i), sig[i]);
			}
			return tuple;
		}
		static Batch fromJson(ObjectNode jo, TWResultSet rs) {
			var batch = new Batch();
			var sig = rs.getOutputSignature();
			batch.exhausted = (jo.has("exhausted"))? jo.get("exhausted").asBoolean() : false;
			var jtuples = (ArrayNode)(jo.get("tuples"));
			batch.tuples = new ArrayList<Object[]>(jtuples.size());
			var it = jtuples.iterator();
			while (it.hasNext()) {
				var jtuple = (ArrayNode)(it.next());
				batch.tuples.add(tupleFromJson(jtuple,sig));				
			}
			return batch;
		}*/
	}
}
