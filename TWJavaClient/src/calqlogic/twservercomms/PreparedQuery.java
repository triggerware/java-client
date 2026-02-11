package calqlogic.twservercomms;

import java.lang.reflect.Constructor;
import java.util.*;
import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;

import calqlogic.twservercomms.QueryStatement.QueryResourceLimits;
import calqlogic.twservercomms.TriggerwareClient.TriggerwareClientException;
import nmg.softwareworks.jrpcagent.*;

/**
 * A PreparedQuery represents a parameterized query that can be issued repeatedly on a connection
 * with different values of its parameters.
 * 
 * Any PreparedQuery instance may only be used on a single connection. That connection is determined by the constructor of the PreparedQuery.
 * 
 * Parameters in an SQL query can be named or positional.  See &lt;link to TW SQL syntax document&gt; for details.
 * 
 * Parameters in an SQL query can be accompanied by static type declarations. 
 * The constructors for PreparedQuery have a parameter of type ParameterDeclarations for this purpose
 * 
 * A PreparedQuery is registered with a TW server.   The constructors require either a TriggerwareConnection
 * or a TriggerwareClient as a parameter of the constructor.  Once the PreparedQuery is registered, each of its parameters
 * must be set (via the setParameter methods) prior to executing the query.
 * 
 * Once the parameters have been set for use  in one execution of the prepared query, they retain their settings for use in
 * further exections unless changed via a setParameter method.
 * 
 * A prepared query, like an ad-hoc query, defines an anonymous table.
 * Executing a prepared query, like executing an ad-hoc query, leads to batches of result rows feeding into a TWResultSet. That result set
 * must be created by invoking the method createResultset.  Depending on the parameters used, the TWResultSet created by that method may or may not
 * contain a batch of result rows.  Normal iteration through that result set will cause its augmentCache method to be invoked to add additional rows 
 * to the result set.  
 * 
 *
 *
 * @param <T> the row type for results of the PreparedQuery
 */
public class PreparedQuery<T> extends AbstractQuery<T> implements Statement{
	
	/**
	 * Declarations of parameters used in a PreparedQuery
	 *
	 */
	private interface ParameterDeclarations {
		void validateParameters() throws TriggerwareClientException;

	}
	/**
	 * Declarations of positional parameters used in a PreparedQuery
	 * Just an ArrayList of names of sql types.  The first element is the name of the parameter with sql index 1.
	 * Null elements are allowed to leave the corresponding query parameter undeclared.
	 *
	 */
	public final static class PositionalParameterDeclarations extends ArrayList<String> implements ParameterDeclarations {
		@Override
		public void validateParameters() throws TriggerwareClientException{}
	}

	/**
	 * Declarations of named parameters used in a PreparedQuery
	 * this is a map from strings that are the query parameter names to values that are the names of sql types for those parameters
	 * Both the parameter names and the type names are treated as case-insensitive.
	 *
	 */
	public final static class NamedParameterDeclarations extends HashMap<String,String> implements ParameterDeclarations {
		@Override
		public void validateParameters() throws TriggerwareClientException {
			for (var type : this.values()) {
				if (!(type == null || TWBuiltInTypes.isValidTypeName(type)))
					throw new TriggerwareClientException("<%s> is not a valid sql type name");
			}			
		}
	}

	private final JavaType crsResultType;

	private boolean usesNamedParameters;

	private  Class<?>[] inputSignatureTypes, outputSignatureTypes;  
	private  String[] inputSignatureNames, inputSignatureTypeNames, outputSignatureNames, outputSignatureTypeNames;
	private  int nparams;
	private int nParamsSet = 0;
	private Object[]paramsByIndex = null;
	private Integer fetchSize;  //null means as many as possible
	private TriggerwareConnection connection;
	//private Set<TWResultSet<T>> outstanding = new HashSet<TWResultSet<T>>();
	private final ParameterDeclarations parameterDeclarations;
	private SignatureElement[]outputSignature = null;
	
	//@Override
	//public void releaseDependentResource(Object resource) {outstanding.remove(resource);}
	public String[] getInputNames() {return inputSignatureNames;}
	public Class<?>[] getInputTypes() {return inputSignatureTypes;}
	Object[] getParameters(){return paramsByIndex;}

	
	/* sample serialization
	  {"handle":4,
	   "inputSignature":[{"attribute":"?col1Min","type":"number"},{"attribute":"?col2Max","type":"number"}],
	   "signature":[{"attribute":"COL1","type":"double"},{"attribute":"COL2","type":"double"}],
	   "usesNamedParameters":true}
	*/
	@JsonFormat(shape=JsonFormat.Shape.OBJECT)
	static class PreparedQueryRegistration{
		final int handle;
		final SignatureElement[]inputSignature;
		final SignatureElement[]rowSignature;
		final boolean usesNamedParameters;
		@JsonCreator
		PreparedQueryRegistration(@JsonProperty("handle")int handle, @JsonProperty("signature")SignatureElement[] signature, 
				            @JsonProperty("inputSignature")SignatureElement[]inputSignature,
				            @JsonProperty("usesNamedParameters")Boolean usesNamedParameters){
			this.handle = handle;
			this.inputSignature = inputSignature;
			this.rowSignature = signature;
			this.usesNamedParameters = usesNamedParameters == null? false : usesNamedParameters;		
		}
		
		
		Class<?>[] inputTypeSignature(){return typeSignatureTypes(inputSignature);}
		Class<?>[] outputTypeSignature(){return typeSignatureTypes(rowSignature);}
		String[] inputTypeNames(){return typeSignatureTypeNames(inputSignature);}
		String[] outputTypeNames(){return typeSignatureTypeNames(rowSignature);}
	}

	@JsonFormat(shape=JsonFormat.Shape.OBJECT)
	static class CreateResultSetResult<T> {//from a prepared query create-resultset
		private final Batch<T> batch;
		private final Integer handle;
		//Batch<T>getBatch(){return batch;}
		//Integer getHandle() {return handle;}
		@JsonCreator 
		public CreateResultSetResult(@JsonProperty(value="handle", required=false) Integer handle, @JsonProperty(value = "batch", required = false) Batch<T> batch){
			 this.batch = batch;
			 this.handle = handle;
		 }
	}

	/**
	 * create a PreparedQuery using SQL syntax and register it for use on a client's primary connection
	 * @param client the client on whose primary connection the query will be used
	 * @param rowClass the row type of the result of the PreparedQuery
	 * @param query  the sql query containing input placeholders
	 * @param schema the default schema for the query
	 * @param parameterDeclarations declarations for any/all parameters used in the query
	 * @throws JRPCException if the server refuses the request to create a prepared query for the query/schema values supplied.
	 */
	public PreparedQuery(TriggerwareClient client, Class<T> rowClass, String query, String schema, ParameterDeclarations parameterDeclarations) throws JRPCException{
		this (client.getPrimaryConnection(), rowClass, query, Language.SQL, schema, parameterDeclarations );}
	
	/**
	 * create a PreparedQuery  register it for use on a client's primary connection
	 * @param client the client on whose primary connection the query will be used
	 * @param rowClass the row type of the result of the PreparedQuery
	 * @param query  the sql query containing input placeholders
	 * @param language the appropriate members of  {@link Language} for the query
	 * @param schema the default schema for the query
	 * @param parameterDeclarations declarations for any/all parameters used in the query
	 * @throws JRPCException if the server refuses the request to create a prepared query for the query/schema values supplied.
	 */
	public PreparedQuery(TriggerwareClient client, Class<T> rowClass, String query, String language, String schema, ParameterDeclarations parameterDeclarations) throws JRPCException{
		this (client.getPrimaryConnection(), rowClass, query, language, schema, parameterDeclarations );}

	/**
	 * create a PreparedQuery and register it for use on a connection
	 * All the other constructors of PreparedQuery simply provide a default value for one or more of the parameters of this constructor.
	 * @param connection the connection on which the prepared query will be used
	 * @param rowClass the row type of the result of the PreparedQuery
	 * @param query  the sql query containing input placeholders
	 * @param language the appropriate members of  {@link Language} for the query
	 * @param schema the default schema for the query
	 * @param parameterDeclarations declarations for any/all parameters used in the query
	 * @throws JRPCException if the server refuses the request to create a prepared query for the query/schema values supplied.
	 */
	PreparedQuery(TriggerwareConnection connection, Class<T> rowClass, String query, String language, String schema, 
			ParameterDeclarations parameterDeclarations) throws JRPCException{
		super(rowClass, query, language, schema);
		this.connection = connection;
		this.parameterDeclarations = parameterDeclarations;
		crsResultType = parametricTypeFor(CreateResultSetResult.class);
		register(connection);
	}

	/*@Override
	protected Object clone() throws CloneNotSupportedException {
		@SuppressWarnings("unchecked")
		var clone = (PreparedQuery<T>)super.clone();
		return clone;
	}*/
	
	static class CreateResultsetRequest<T> extends NamedParameterRequest<CreateResultSetResult<T>>{
		private final Constructor<T> rowConstructor;
		private final SignatureElement[] rowSignature;
		private static String[]requiredParams = new String[] {"handle", "inputs"},
				               optionalParams = new String[] {"limit", "timelimit", "check-update"};
		CreateResultsetRequest(JavaType responseType, Constructor<T> rowConstructor, SignatureElement[] rowSignature){
			super(responseType, "create-resultset",  requiredParams, optionalParams);
			this.rowConstructor = rowConstructor;
			this.rowSignature = rowSignature;
		}
		Constructor<T> getRowConstructor(){return rowConstructor;}
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
			dsstate.put("rowSignature", rowSignature);
			if (rowConstructor != null) {
				dsstate.put("rowBeanConstructor", rowConstructor);
			}
		}
	}
	static class NotificationResultsRequest<T> extends NamedParameterRequest<CreateResultSetResult<T>>{
		private final Constructor<T> rowConstructor;
		private final SignatureElement[] signature;
		private static final String[]requiredParams = new String[] {"handle", "method" },
				              optionalParams  = new String[] {"limit", "timelimit", "notify-limit", "notify-timelimit"};

		NotificationResultsRequest(JavaType responseType, /*Class<?>rowClass,*/ Constructor<T> rowConstructor, SignatureElement[] signature){
			super(responseType, "create-resultset-incremental",  requiredParams, optionalParams);
			this.rowConstructor = rowConstructor;
			this.signature = signature;
		}
		Constructor<T> getRowConstructor(){return rowConstructor;}

		/**
		 * establishResponseDeserializationAttributes is used internally to establish context used to deserialize JRPC messages. It is not intended to be overridden or called in application code.
		 * @param request
		 * @param response
		 * @param ctxt
		 */
		@Override // in the case of rows sent by notification, this method is called during deserialization of the notification, not during deserialization of the request's response
		public void establishResponseDeserializationAttributes(JRPCSimpleRequest<?> request, IncomingMessage response, DeserializationContext ctxt) {
			if (rowConstructor != null) {
				@SuppressWarnings("unchecked")
				var dsstate= (HashMap<String,Object>)ctxt.getAttribute("deserializationState");
				dsstate.put("rowBeanConstructor", rowConstructor);
				dsstate.put("rowSignature", signature);
			}
		}
	}
	
	/**
	 * create a clone of this prepared query that is registered on the same connection as this query
	 * @return the cloned prepared query
	 * @throws JRPCException error communicating with the TW server
	 */
	public synchronized PreparedQuery<T> cloneWithParameters() throws JRPCException{
		return cloneWithParameters(this.connection);	}
	
	/**
	 * create a clone of this prepared query 
	 * @param c  the connecton on which the clone will be registered
	 * @return the cloned prepared query
	 * @throws JRPCException
	 */
	@SuppressWarnings("unchecked")
	private PreparedQuery<T> cloneWithParameters(TriggerwareConnection c) throws JRPCException{
		try {
			PreparedQuery<T>clone = (PreparedQuery<T>)this.clone();
			clone.paramsByIndex = paramsByIndex.clone();
			clone.connection = c;
			connection.addPreparedQuery(clone);
			//tell TW to give us a handle for clone, using what it knows about this.twHandle
			//clone.twHandle = handle obtained from tw
			return clone;
		} catch (CloneNotSupportedException e) {//impossible
			return null;
		}
	}
	
	private  void register(TriggerwareConnection connection) throws JRPCException {
		var params = new NamedRequestParameters().with("query", query).with("namespace", schema).with("language", language);
		if (parameterDeclarations != null)		params = params.with("parameter-types", parameterDeclarations);
		var pqresult = (PreparedQueryRegistration)connection.synchronousRPC(PreparedQueryRegistration.class,  "prepare-query", params);
		registered(pqresult, connection);
	}
	private void registered(PreparedQueryRegistration pqResult, TriggerwareConnection connection) {;
		recordRegistration(connection, pqResult.handle);
		connection.addPreparedQuery(this);
		fetchSize = ((TriggerwareClient)(connection.getAgent())).getDefaultFetchSize();
		inputSignatureTypes = pqResult.inputTypeSignature();
		inputSignatureTypeNames = pqResult.inputTypeNames();
		outputSignature = pqResult.rowSignature;
		outputSignatureNames = signatureNames(pqResult.rowSignature);
		outputSignatureTypes = pqResult.outputTypeSignature();
		outputSignatureTypeNames = pqResult.outputTypeNames();
		usesNamedParameters = pqResult.usesNamedParameters;
		nparams = inputSignatureTypes.length;
		if (usesNamedParameters) {
			inputSignatureNames = signatureNames(pqResult.inputSignature);
		} else {
			inputSignatureNames = null;	}
		paramsByIndex = new Object[nparams];
		clearParameters();		
	}

	/**
	 * set the fetchSize  for executions of this PreparedQuery.  The fetch size is the number of results to return in
	 * a ResultSet from executing the query, as well as the initial setting of that ResultSet's own fetchSize.
	 * @param fetchSize the fetchSize to use
	 * @return the previous setting of fetchSize for this PreparedQuery
	 */
	public synchronized Integer setFetchSize(Integer fetchSize) {
		var old = this.fetchSize;
		this.fetchSize = fetchSize;
		return old;
	}
	/**
	 * @return the current fetchSize for this PreparedQuery
	 */
	public Integer getFetchSize() {return  fetchSize;}
	
	private int indexOfParameterName(String paramName) {
		//returns a 0-based index
		int i = 0;
		for (var iname : this.inputSignatureNames)
			if (iname.equalsIgnoreCase(paramName)) return i; else i++;
		return -1;
	}
	
	private static Object unset = new Object();

	/**
	 * clear the parameter settings for this PreparedQuery
	 */
	public synchronized void clearParameters() {
		Arrays.fill(paramsByIndex, unset);
		nParamsSet = 0;
	}
	
	synchronized boolean fullyInstantiated() {
		return nParamsSet == nparams;}
	
	private void checkTypeOfParameterSetting(int parameterIndex, Object paramValue, Object parameterNameOrIndex)throws TriggerwareClientException {
		var type = inputSignatureTypes[parameterIndex];
		if (! (paramValue == null || type.isInstance(paramValue)))
			throw new TriggerwareClientException(
					String.format("prepared query parameter type error for parameter  %s, <%s> expected", parameterNameOrIndex, type.getName()));		
	}

	/**
	 * Set a parameter of a PreparedQuery that used positional parameters.
	 * @param parameterIndex the 1-based index of the parameter to set
	 * @param paramValue  the parameter value to use
	 * @return this PreparedQuery.  This is useful for chaining the setting of multiple parameters.
	 * @throws TriggerwareClientException if this PreparedQuery uses named parameters, or if the parameterIndex is out of bounds,
	 * or if the parameter value is not of an acceptable type for the parameter's uses in the query.
	 */
	public synchronized PreparedQuery<T> setParameter(int parameterIndex, Object paramValue) throws TriggerwareClientException {
		if (usesNamedParameters)
			throw new TriggerwareClientException("cannot set parameter of a named parameter PreparedQuery via an integer index");
		if (parameterIndex<1 || parameterIndex> nparams)
			throw new TriggerwareClientException(String.format("prepared query parameter index out of bounds. Parameter indices are 1 through %d", nparams));
		checkTypeOfParameterSetting(parameterIndex-1, paramValue, parameterIndex);
		var old = paramsByIndex[parameterIndex-1];
		paramsByIndex[parameterIndex-1] = paramValue;
		if (old == unset) nParamsSet++;
		return this;
	}

	/**
	 * Set a parameter of a PreparedQuery that used named parameters.
	 * @param parameterName the name of the parameter to set
	 * @param paramValue  the parameter value to use
	 * @return this PreparedQuery.  This is useful for chaining the setting of multiple parameters.
	 * @throws TriggerwareClientException if this PreparedQuery uses positional parameters, 
	 * or if the parameterName is not the name of one of this PreparedQuery's parameters,
	 * or if the parameter value is not of an acceptable type for the parameter's uses in the query.
	 */
	public synchronized PreparedQuery<T> setParameter(String parameterName, Object paramValue) throws TriggerwareClientException {
		if (!usesNamedParameters)
			throw new TriggerwareClientException("cannot set parameter of an indexed parameter PreparedQuery via a parameter name");
		int parameterIndex = this.indexOfParameterName(parameterName);
		if (parameterIndex == -1)
			throw new TriggerwareClientException(String.format("unknown prepared query parameter name <%s>", parameterName));

		checkTypeOfParameterSetting(parameterIndex, paramValue, parameterName);
		/*var type = this.inputSignatureTypes[parameterIndex];
		if (! (paramValue == null || type.isInstance(paramValue)))
			throw new TriggerwareClientException(
					String.format("prepared query parameter type error for parameter name <%s>", parameterName));*/
		var old = paramsByIndex[parameterIndex];
		paramsByIndex[parameterIndex] = paramValue;
		if (old == unset) nParamsSet++;
		return this;
	}

	/**
	 * Execute this prepared query to obtain a TWResultSet.
	 * @return a TWResultSet with the first batch of results from executing this PreparedQuery
	 * @throws JRPCException if the server responds with an exception
	 * @throws TriggerwareClientException if not all the parameters of this PreparedQuery have been set.
	 */
	public TWResultSet<T> createResultset() throws JRPCException, TriggerwareClientException {
		return createResultset(null);	}
	
	/**
	 * execute this PreparedQuery (whose parameters have been set) to obtain a resultset holding the first batch of results for those parameters.
	 * @param qrl QueryResourceLimits for executing the query.
	 * @return a TWResultSet with the first batch of results from executing this PreparedQuery
	 * @throws JRPCException if the server responds with an error
	 * @throws TriggerwareClientException if not all the parameters of this PreparedQuery have been set.
	 */
	public TWResultSet<T> createResultset(QueryStatement.QueryResourceLimits qrl)  throws JRPCException, TriggerwareClientException {
		TWResultSet<T> rs = createResultSet(qrl);
		//outstanding.add(rs);
		return rs;
	}

	/**
	 * @param controller controls that affect the timing/batching of results
	 * @throws JRPCException if the server responds with an error
	 * @throws TriggerwareClientException if this prepared query has been closed or is current executing
	 */
	public void executeQueryWithNotificationResults(NotificationResultController<T> controller) 
			throws JRPCException, TriggerwareClientException {
		    if (closed) throw  new TriggerwareClientException("attempt to execute a closed PreparedStatement");
			if (controller.getHandle() != null)
				throw  new TriggerwareClientException("using an open NotificationResultController with a new query.");
			if (!fullyInstantiated())
				throw new TriggerwareClientException("cannot execute a prepared query without setting all the parameters");
			var rs = this.createResultset(new QueryResourceLimits().withLimit(0)); 
			controller.setHandle(connection, rs.getHandle(), this.outputSignature, this.rowConstructor, rs);
	
			//now start streaming
			var nriParams = controller.controlParams();
			connection.synchronousRPC(Void.TYPE, "next-resultset-incremental", nriParams); //tell server to start streaming
	}

	/**
	 * @return <code>true</code> if this PreparedQuery uses named parameters, <code>false</code> if it uses positional parameters
	 * or if it has no parameters.
	 */
	public boolean usesNamedParameters() {return usesNamedParameters;}
	
	public String[] getOutputSignatureNames(){return outputSignatureNames;} //used only in QueryBuilder. Move 
	public Class<?>[] getOutputSignatureTypes(){return outputSignatureTypes;} //used only in QueryBuilder. Move 
	public String[] getOutputSignatureTypeNames(){return outputSignatureTypeNames;}
	
	public String[] getInputSignatureNames(){return inputSignatureNames;}
	public Class<?>[] getInputSignatureTypes(){return inputSignatureTypes;}
	public String[] getInputSignatureTypeNames(){return inputSignatureTypeNames;}

	/**
	 * execute this prepared query on the TW server to obtain a TWResultSet with an initial batch of results
	 * @param qrl the resource limits for the server
	 * @return a new TWResultSet built from the initial batch of results
	 * @throws JRPCException
	 * @throws TriggerwareClientException
	 */
	public TWResultSet<T> createResultSet(QueryStatement.QueryResourceLimits qrl) throws JRPCException, TriggerwareClientException {
		NamedRequestParameters crParams;
		synchronized(this) {
			if (closed)
				throw new TriggerwareClientException("attempt to execute a closed PreparedQuery");
			if (!fullyInstantiated())
				throw new TriggerwareClientException("cannot execute a prepared query without setting all the parameters");

			var timeout = (qrl == null) ? null :qrl.getTimeout();
			var rcl = qrl.getRowCountLimit();
			Integer fetchSize = this.fetchSize;
			if (fetchSize == null)  fetchSize = rcl;
			if (fetchSize != null && rcl != null && rcl<fetchSize) fetchSize = rcl;
			
			
			if (qrl != null && qrl.getRowCountLimit() != null) 
				fetchSize = qrl.getRowCountLimit();
			crParams = new NamedRequestParameters().with("handle", twHandle).with("limit", fetchSize)
					.with ("timelimit", timeout)
					.with("inputs", paramsByIndex).with("check-update", false);
		}
		var crNPR = new CreateResultsetRequest<T>(crsResultType, /*rowClass,*/ rowConstructor, outputSignature);
		var crResult = (CreateResultSetResult<T>)connection.synchronousRPC(crNPR, crParams);
		//crResult.setHandle(this.twHandle);
		/*if (timeout != null) { 
			var future = connection.asynchronousRPCN(crsResultType, null, true, "create-resultset",  params);
			try {
				eqresult = (ExecuteQueryResult<T>)JRPCAsyncRequest.executeWithTimeout(future, (long)(timeout*1000));
			} catch (TimeoutException e) {
				future.cancel(false); //that will cause the agent to ignore the eventual result.
				throw new TimeoutException("client timeout  waiting for prepared query result");
			}catch(CancellationException e) {//this is a runtime exception in java
				throw e;
			}catch (InterruptedException | ExecutionException  e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
			}
		}
		else 
			eqresult = connection.synchronousRPC(crsResultType, null, "create-resultset", params);*/
		return new TWResultSet<T>(rowConstructor, connection, crResult.handle, fetchSize, qrl.getRowCountLimit(), outputSignature, crResult.batch.getRows());
	}
	
	/**  obtain a resultset for this prepared query, providing no resource limits
	 * @return a resultset for this prepared query
	 * @throws JRPCException
	 * @throws TriggerwareClientException
	 */
	public TWResultSet<T> createResultSet() throws JRPCException, TriggerwareClientException {
		return createResultSet(null);}

	private static PositionalParameterRequest<Void> releaseQueryRequest = 
			new PositionalParameterRequest<Void>(Void.TYPE, "release-query", 1, 1);

	@Override
	public synchronized void close() {
		if (closed) return;// false;
		try {
			//close any resultsets
			//for (var rs : outstanding) rs.close();
			//outstanding.clear();
			closed = true;
			releaseQueryRequest.execute(connection, twHandle);
			connection.removePreparedQuery(this);
			//return true;
		} catch (JRPCException e) {
		   Logging.log("error closing a PreparedQuery <%s>", e.getMessage());
		   //return false;
		}		
	}
}
