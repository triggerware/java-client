package calqlogic.twservercomms;

import java.io.UncheckedIOException;
import java.util.*;
import java.util.concurrent.TimeoutException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JavaType;
import calqlogic.twservercomms.QueryStatement.ExecuteQueryResult;
import calqlogic.twservercomms.TriggerwareClient.TriggerwareClientException;
import nmg.softwareworks.jrpcagent.JRPCException;
import nmg.softwareworks.jrpcagent.Logging;
import nmg.softwareworks.jrpcagent.NamedRequestParameters;
import nmg.softwareworks.jrpcagent.PositionalParameterRequest;

/**
 * A PreparedQuery represents a parameterized query that can be issued repeatedly on a connection
 * with different values of its parameters.
 * <p>
 * Parameters in an SQL query can be named or positional.  See &lt<link to TW SQL syntax document> for details.
 *
 * @param <T> the row type for results of the PreparedQuery
 */
public class PreparedQuery<T> extends AbstractQuery<T> implements Statement, Cloneable {
    private final JavaType crsResultType;
    //@JsonProperty("usesNamedParameters")
    private boolean usesNamedParameters;

    private Class<?>[] inputSignatureTypes, outputSignatureTypes;
    private String[] inputSignatureNames, inputSignatureTypeNames, outputSignatureNames, outputSignatureTypeNames;
    private int nparams;
    private int nParamsSet = 0;
    private Object[] paramsByIndex = null;
    private Integer fetchSize;  //null means as many as possible
    private final Set<TWResultSet<T>> outstanding = new HashSet<TWResultSet<T>>();

    //@Override
    //public void releaseDependentResource(Object resource) {outstanding.remove(resource);}
    public String[] getInputNames() {
        return inputSignatureNames;
    }

    public Class<?>[] getInputTypes() {
        return inputSignatureTypes;
    }

    Object[] getParameters() {
        return paramsByIndex;
    }


    /* sample serialization
      {"handle":4,
       "inputSignature":[{"attribute":"?col1Min","type":"number"},{"attribute":"?col2Max","type":"number"}],
       "signature":[{"attribute":"COL1","type":"double"},{"attribute":"COL2","type":"double"}],
       "usesNamedParameters":true}
    */
    @JsonFormat(shape = JsonFormat.Shape.OBJECT)
    static class PreparedQueryRegistration {
        final int handle;
        final SignatureElement[] inputSignature;
        final SignatureElement[] signature;
        final boolean usesNamedParameters;

        @JsonCreator
        PreparedQueryRegistration(@JsonProperty("handle") int handle,
                                  @JsonProperty("signature") SignatureElement[] signature,
                                  @JsonProperty("inputSignature") SignatureElement[] inputSignature,
                                  @JsonProperty("usesNamedParameters") boolean usesNamedParameters) {
            this.handle = handle;
            this.inputSignature = inputSignature;
            this.signature = signature;
            this.usesNamedParameters = usesNamedParameters;
        }

        Class<?>[] inputTypeSignature() {
            return typeSignatureTypes(inputSignature);
        }

        Class<?>[] outputTypeSignature() {
            return typeSignatureTypes(signature);
        }


        String[] inputTypeNames() {
            return typeSignatureTypeNames(inputSignature);
        }

        String[] outputTypeNames() {
            return typeSignatureTypeNames(signature);
        }
    }

    private PreparedQuery(TriggerwareConnection connection, Object rowType, String query, String language, String schema) {
        super(rowType, query, language, schema);
        crsResultType = parametricTypeFor(connection.getClient(), QueryStatement.ExecuteQueryResult.class);
    }


    /**
     * create a PreparedQuery and register it for use on a connection
     *
     * @param rowType    the row type of the result of the PreparedQuery
     * @param query      the sql query containing input placeholders
     * @param schema     the default schema for the query
     * @param connection the connection on which this prepared query will be registered and later used
     * @throws JRPCException if the server refuses the request to create a prepared query for the query/schema values supplied.
     */
    public PreparedQuery(Object rowType, String query, String schema,
                         TriggerwareConnection connection) throws JRPCException {
        this(connection, rowType, query, Language.SQL, schema);
        registerNoCheck(connection);
    }

    /**
     * create a PreparedQuery and register it for use on a connection
     *
     * @param rowType    the row type of the result of the PreparedQuery
     * @param query      the sql query containing input placeholders
     * @param language   the appropriate members of  {@link Language} for the query
     * @param schema     the default schema for the query
     * @param connection the connection on which this prepared query will be registered and later used
     * @throws JRPCException if the server refuses the request to create a prepared query for the query/schema values supplied.
     */
    public PreparedQuery(Object rowType, String query, String language, String schema,
                         TriggerwareClient client) throws JRPCException {
        this(rowType, query, language, schema, client.getPrimaryConnection());
    }

    /**
     * create a PreparedQuery and register it for use on a connection
     *
     * @param rowType    the row type of the result of the PreparedQuery
     * @param query      the sql query containing input placeholders
     * @param language   the appropriate members of  {@link Language} for the query
     * @param schema     the default schema for the query
     * @param connection the connection on which this prepared query will be registered and later used
     * @throws JRPCException if the server refuses the request to create a prepared query for the query/schema values supplied.
     */
    public PreparedQuery(Object rowType, String query, String language, String schema,
                         TriggerwareConnection connection) throws JRPCException {
        this(connection, rowType, query, language, schema);
        registerNoCheck(connection);
    }

	/*@Override
	protected Object clone() throws CloneNotSupportedException {
		@SuppressWarnings("unchecked")
		var clone = (PreparedQuery<T>)super.clone();
		return clone;
	}*/

    /**
     * create a clone of this prepared query that is registered on the same connection as this query
     *
     * @return the cloned prepared query
     * @throws JRPCException error communicating with the TW server
     */
    public synchronized PreparedQuery<T> cloneWithParameters() throws JRPCException {
        return cloneWithParameters(this.connection);
    }

    /**
     * create a clone of this prepared query
     *
     * @param c the connecton on which the clone will be registered
     * @return the cloned prepared query
     * @throws JRPCException
     */
    @SuppressWarnings("unchecked")
    private PreparedQuery<T> cloneWithParameters(TriggerwareConnection c) throws JRPCException {
        try {
            PreparedQuery<T> clone = (PreparedQuery<T>) this.clone();
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

    /**
     * register a prepared query for use on a connection
     *
     * @param connection the connection on which the query will be used
     * @throws JRPCException for problems signalled by the TW Server, such as invalid syntax in the query.
     */
    public synchronized void register(TriggerwareConnection connection) throws JRPCException {
        if (this.connection != null)
            throw new ReregistrationError();
        registerNoCheck(connection);
    }

    private void registerNoCheck(TriggerwareConnection connection) throws JRPCException {
        //Object[]params = new Object[] {query, schema, language};
        var params = new NamedRequestParameters().with("query", query).with("namespace", schema).with("language", language);
        var pqresult = (PreparedQueryRegistration) connection.synchronousRPC(PreparedQueryRegistration.class, null, null, "prepare-query", params);
        registered(pqresult, connection);
    }

    /**
     * create a PreparedQuery and register it for use on a client's primary connection
     *
     * @param rowType the row type of the result of the PreparedQuery
     * @param query   the sql query containing input placeholders
     * @param schema  the default schema for the query
     * @param client  the client that will use this query on its primary connection
     * @throws JRPCException if the server refuses the request to create a prepared query for the query/schema values supplied.
     */
    public PreparedQuery(Object rowType, String query, String schema,
                         TriggerwareClient client) throws JRPCException {
        this(rowType, query, Language.SQL, schema, client.getPrimaryConnection());
    }

    private void registered(PreparedQueryRegistration pqResult, TriggerwareConnection connection) {
        recordRegistration(connection, pqResult.handle);
        connection.addPreparedQuery(this);
        fetchSize = connection.getAgent().getDefaultFetchSize();
        inputSignatureTypes = pqResult.inputTypeSignature();
        inputSignatureTypeNames = pqResult.inputTypeNames();
        outputSignatureNames = signatureNames(pqResult.signature);
        outputSignatureTypes = pqResult.outputTypeSignature();
        outputSignatureTypeNames = pqResult.outputTypeNames();
        usesNamedParameters = pqResult.usesNamedParameters;
        nparams = inputSignatureTypes.length;
        if (usesNamedParameters) {
            inputSignatureNames = signatureNames(pqResult.inputSignature);
        } else {
            inputSignatureNames = null;
        }
        paramsByIndex = new Object[nparams];
        clearParameters();
    }

    /**
     * set the fetchSize  for executions of this PreparedQuery.  The fetch size is the number of results to return in
     * a ResultSet from executing the query, as well as the initial setting of that ResultSet's own fetchSize.
     *
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
    public Integer getFetchSize() {
        return fetchSize;
    }

    private int indexOfParameterName(String paramName) {
        //returns a 0-based index
        int i = 0;
        for (var iname : this.inputSignatureNames)
            if (iname.equalsIgnoreCase(paramName)) return i;
            else i++;
        return -1;
    }

    private static final Object unset = new Object();

    /**
     * clear the parameter settings for this PreparedQuery
     */
    public synchronized void clearParameters() {
        Arrays.fill(paramsByIndex, unset);
        nParamsSet = 0;
    }

    synchronized boolean fullyInstantiated() {
        return nParamsSet == nparams;
    }

    /**
     * Set a parameter of a PreparedQuery that used positional parameters.
     *
     * @param parameterIndex the 1-based index of the parameter to set
     * @param paramValue     the parameter value to use
     * @return this PreparedQuery.  This is useful for chaining the setting of multiple parameters.
     * @throws TriggerwareClientException if this PreparedQuery uses named parameters, or if the parameterIndex is out of bounds,
     *                                    or if the parameter value is not of an acceptable type for the parameter's uses in the query.
     */
    public synchronized PreparedQuery<T> setParameter(int parameterIndex, Object paramValue) throws TriggerwareClientException {
        if (usesNamedParameters)
            throw new TriggerwareClientException("cannot set parameter of a named parameter PreparedQuery via an integer index");
        if (parameterIndex < 1 || parameterIndex > nparams)
            throw new TriggerwareClientException("prepared query parameter index out of bounds");
        var type = inputSignatureTypes[parameterIndex - 1];
        if (!(type.isInstance(paramValue)))
            throw new TriggerwareClientException(
                    String.format("prepared query parameter type error for parameter index %d", parameterIndex));
        var old = paramsByIndex[parameterIndex - 1];
        paramsByIndex[parameterIndex - 1] = paramValue;
        if (old == unset) nParamsSet++;
        return this;
    }

    /**
     * Set a parameter of a PreparedQuery that used named parameters.
     *
     * @param parameterName the name of the parameter to set
     * @param paramValue    the parameter value to use
     * @return this PreparedQuery.  This is useful for chaining the setting of multiple parameters.
     * @throws TriggerwareClientException if this PreparedQuery uses positional parameters,
     *                                    or if the parameterName is not the name of one of this PreparedQuery's parameters,
     *                                    or if the parameter value is not of an acceptable type for the parameter's uses in the query.
     */
    public synchronized PreparedQuery<T> setParameter(String parameterName, Object paramValue) throws TriggerwareClientException {
        if (!usesNamedParameters)
            throw new TriggerwareClientException("cannot set parameter of an indexed parameter PreparedQuery via a parameter name");
        int parameterIndex = this.indexOfParameterName(parameterName);
        if (parameterIndex == -1)
            throw new TriggerwareClientException(String.format("unknown prepared query parameter name <%s>", parameterName));
        var type = this.inputSignatureTypes[parameterIndex];
        if (!(type.isInstance(paramValue)))
            throw new TriggerwareClientException(
                    String.format("prepared query parameter type error for parameter name <%s>", parameterName));
        var old = paramsByIndex[parameterIndex];
        paramsByIndex[parameterIndex] = paramValue;
        if (old == unset) nParamsSet++;
        return this;
    }
	
	/*Object[] inputsAsArray() throws TriggerwareClientException{
		if (!fullyInstantiated())
			throw new TriggerwareClientException("cannot execute a prepared query without setting all the parameters");
		return paramsByIndex;
	}*/

    /**
     * @return a TWResultSet with the first batch of results from executing this PreparedQuery
     * @throws JRPCException              if the server responds with an exception
     * @throws TriggerwareClientException if not all the parameters of this PreparedQuery have been set.
     */
    public TWResultSet<T> executeQuery() throws JRPCException, TriggerwareClientException {
        try {
            TWResultSet<T> rs = createResultSet(null);
            outstanding.add(rs);
            return rs;
        } catch (TimeoutException e) { //not  possible with no resource limit
            return null;
        }
    }

    /**
     * @param qrl -- QueryResourceLimits for executing the query.
     * @return a TWResultSet with the first batch of results from executing this PreparedQuery
     * @throws JRPCException              if the server responds with an error
     * @throws TriggerwareClientException if not all the parameters of this PreparedQuery have been set.
     * @throws TimeoutException           if the resource limits specify a timeout
     */
    public TWResultSet<T> executeQuery(QueryStatement.QueryResourceLimits qrl)
            throws JRPCException, TriggerwareClientException, TimeoutException {
        TWResultSet<T> rs = createResultSet(qrl);
        outstanding.add(rs);
        return rs;
    }

    /**
     * @param controller controls that affect the timing/batching of results
     * @throws JRPCException              if the server responds with an error
     * @throws TriggerwareClientException if this prepared query has been closed or is current executing
     */
    public void executeQueryWithPushResults(PushResultController<T> controller)
            throws JRPCException, TriggerwareClientException {
        if (closed) throw new TriggerwareClientException("attempt to execute a closed PreparedStatement");
        if (controller.getHandle() != null)
            throw new TriggerwareClientException("using an open PushResultController with a new query.");
        if (!fullyInstantiated())
            throw new TriggerwareClientException("cannot execute a prepared query without setting all the parameters");

        //now do create-resultset request with count = 0
        var crParams = new NamedRequestParameters().with("handle", twHandle).with("limit", 0)
                .with("inputs", paramsByIndex).with("check-update", false);
        ExecuteQueryResult<T> eqresult = connection.synchronousRPC(crsResultType, null, null, "create-resultset", crParams);
        controller.setHandle(connection, eqresult.getHandle()); //that registers the handler
        var nriParams = controller.getParams();
        //now start streaming
        connection.synchronousRPC(Void.TYPE, null, null, "next-resultset-incremental", nriParams); //tell server to start streaming
    }

    /**
     * @return <code>true</code> if this PreparedQuery uses named parameters, <code>false</code> if it uses positional parameters
     * or if it has no parameters.
     */
    public boolean usesNamedParameters() {
        return usesNamedParameters;
    }

    public String[] getOutputSignatureNames() {
        return outputSignatureNames;
    }

    public Class<?>[] getOutputSignatureTypes() {
        return outputSignatureTypes;
    }

    public String[] getOutputSignatureTypeNames() {
        return outputSignatureTypeNames;
    }

    public String[] getInputSignatureNames() {
        return inputSignatureNames;
    }

    public Class<?>[] getInputSignatureTypes() {
        return inputSignatureTypes;
    }

    public String[] getInputSignatureTypeNames() {
        return inputSignatureTypeNames;
    }

    //@SuppressWarnings("unchecked")
    private TWResultSet<T> createResultSet(QueryStatement.QueryResourceLimits qrl) throws JRPCException, TriggerwareClientException, TimeoutException {
        NamedRequestParameters params;
        Integer fetchSize;
        synchronized () {
            if (closed)
                throw new TriggerwareClientException("attempt to execute a closed PreparedQuery");
            if (!fullyInstantiated())
                throw new TriggerwareClientException("cannot execute a prepared query without setting all the parameters");

            fetchSize = this.fetchSize;
            var timeout = (qrl != null) ? qrl.getTimeout() : null;
            if (qrl != null && qrl.getRowCountLimit() != null)
                fetchSize = qrl.getRowCountLimit();
            params = new NamedRequestParameters().with("handle", twHandle).with("limit", fetchSize)
                    .with("timelimit", timeout)
                    .with("inputs", paramsByIndex).with("check-update", false);
        }
        //if (qrl != null && qrl.getTimeout() != null) 
        //	params.with("timelimit", qrl.getTimeout());
        //var jt = connection.getTypeFactory().constructParametricType(QueryStatement.ExecuteQueryResult.class, rowType);
        ExecuteQueryResult<T> eqresult = null;
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
		else */
        eqresult = connection.synchronousRPC(crsResultType, null, null, "create-resultset", params);
        return new TWResultSet<T>(this.rowClass, this, fetchSize, eqresult, this);
    }

    private static final PositionalParameterRequest<Void> releaseQueryRequest =
            new PositionalParameterRequest<Void>(Void.TYPE, null, "release-query", 1, 1);

    @Override
    public void close() {
        closeQuery();
    }

    @Override
    public synchronized boolean closeQuery() {
        if (closed) return false;
        try {
            for (var rs : outstanding)
                rs.close();
            outstanding.clear();
            closed = true;
            releaseQueryRequest.execute(connection, twHandle);
            connection.removePreparedQuery(this);
            return true;
        } catch (JRPCException e) {
            Logging.log("error closing a PreparedQuery <%s>", e.getMessage());
            return false;
        }
    }
}
