package calqlogic.twservercomms;

import java.time.Duration;
import java.time.Instant;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.TypeFactory;

import calqlogic.twservercomms.TriggerwareClient.TriggerwareClientException;
import nmg.softwareworks.jrpcagent.*;

/**
 * <p>
 * A PolledQuery is a query that will be executed by the TW server multiple times. The first time it is executed
 * the answer (a set of 'rows') establishes a 'current state' of the query. For each succeeding execution (referred to
 * as <em>polling</em> the query), </p>
 * <ul>
 * <li> the new answer is compared with the current state, and the differences are sent to the triggerware client
 * in a notification containing a {@link RowsDelta} value.
 * <li> the new answer then becomes the current state to be used for comparison with the result of the next poll of the query.
 * </ul>
 * <p> Like any other query, a PolledQuery has a query string, a language (FOL or SQL), and a namespace.
 * </p><p> A polling operation may be performed at any time by executing the {@link #poll()} method.
 * Some details of reporting and polling can be configured with a {@link PolledQuery.PolledQueryControlParameters}
 * value that is supplied to the constructor of a PolledQuery.
 * </p><p>
 * An instantiable subclass of PolledQuery must provide a {@link #handleSuccess} method to deal with notifications of
 * changes to the current state.  There are errors that can occur during a polling operation (timeout, inability to contact
 * a data source).  When such an error occurs, the TW Server will send an "error" notification.
 * An instantiable subclass of PolledQuery may provide a {@link #handleError} method to deal with error notifications.
 * </p><p>
 * {@link ScheduledQuery} is a subclass of PolledQuery which provides for polling to occur based on a schedule, rather than
 * solely on demand (ie, solely by virtue of the client submitting a poll request to the TW server). The existence of
 * scheduled queries is the reason that state deltas are sent to the client as notifications, even when a poll is explicitly
 * requested. Scheduled queries may still be polled on demand at any time.
 * </p><p>
 * Polling may be terminated by {@link #closeQuery}
 * </p><p>
 * If a polling operation is ready to start (whether due to its schedule or an explicit poll request) and a previous poll of
 * the query has not completed, the poll operation that is ready to start is simply skipped, and an error notification is
 * sent to the client.
 * </p>
 *
 * @param <T> the class that repesents a single 'row' of the answer to the query.
 * @see ScheduledQuery
 */
public abstract class PolledQuery<T> extends AbstractQuery<T> implements NotificationInducer {
    protected final String notificationMethod = TriggerwareClient.nextNotificationMethod("pq");
    protected Class<?>[] signatureTypes;
    protected String[] signatureNames, signatureTypeNames;
    protected final PolledQuerySchedule schedule;
    protected final PolledQueryControlParameters controls;
    private Object notificationType; // set when the TriggerwareClient is known
    //private boolean isRegistered = false;
    protected boolean hasSucceeded = false;
    protected final PreparedQuery<T> preparedQuery;

    /**
     * PolledQueryControlParameters
     */
    public static class PolledQueryControlParameters {
        /**
         * reportUnchanged determines whether a success notification is sent to the TriggerwareClient when a poll of the query
         * returns the same set of values as the current state.  The delta value for such a notification would have empty
         * sets of rows for both the added and deleted sets.
         * The default is false, meaning <em>not</em> to send such notifications.		 *
         */
        public final boolean reportUnchanged;
        /**
         * pollTimeout is a limit on how much time should be allowed for a polling operation.
         * If the poll exceeds this time limit, it is effectively aborted (on the tw server) and
         * an error notification is sent to the TriggerwareClient.
         * The default is null, meaning that no time limit is used.
         */
        public final Duration pollTimeout;
        /**
         * reportInitial determines whether the success notification sent when the first successful poll operation completes
         * will contain the row values that constitute the initial state.
         * Such a notification will always have an empty set of 'deleted' rows.  The only means to reliably distinguish
         * this from other success notifications is to use the hasSucceeded method.
         * The default value is false,  meaning <em>not</em> to include rows in that notification.
         */
        public final boolean reportInitial; //should the initial delta be reported with the initial polling notification

        private PolledQueryControlParameters() {
            reportUnchanged = false;
            pollTimeout = null;
            reportInitial = false;
        }

        /**
         * Construct a PolledQueryControlParameters instance providing a value for each parameter.
         *
         * @param reportUnchanged the value for the reportUnchanged parameter
         * @param reportInitial   the value for the reportInitial parameter
         * @param pollTimeout     the value for the pollTimeout parameter
         */
        public PolledQueryControlParameters(boolean reportUnchanged, boolean reportInitial, Duration pollTimeout) {
            this.reportUnchanged = reportUnchanged;
            this.pollTimeout = pollTimeout;
            this.reportInitial = reportInitial;
        }
    }

    /**
     * defaultControlParameters is a PolledQueryControlParameters instance with the default value for each parameter.
     */
    public static PolledQueryControlParameters defaultControlParameters = new PolledQueryControlParameters();

    @JsonFormat(shape = JsonFormat.Shape.OBJECT)
    static class PolledQueryRegistration {//the result of registering a polled query
        final int handle;
        final SignatureElement[] signature;

        @JsonCreator
        PolledQueryRegistration(@JsonProperty("handle") int handle, @JsonProperty("signature") SignatureElement[] signature) {
            this.handle = handle;
            this.signature = signature;
        }

        Class<?>[] typeSignature() {
            return AbstractQuery.typeSignatureTypes(signature);
        }

        String[] typeNames() {
            return typeSignatureTypeNames(signature);
        }

        String[] attributeNames() {
            return signatureNames(signature);
        }
    }

    /**
     * @param rowType    the java class into which each row of a delta will be deserialized.
     * @param query      the sql query to be polled
     * @param schema     the default schema for resolving table names in the query.
     * @param schedule   the schedule for polling the query. For use with ScheduledQuery and subclasses thereof
     * @param connection the connection on which the polled query's notifications will be delivered
     * @param controls   the control parameters to use for polling and reporting.
     * @throws JRPCException if registration on the connection fails
     */
    protected PolledQuery(Class<?> rowType, String query, String schema,
                          TriggerwareConnection connection, PolledQuerySchedule schedule, PolledQueryControlParameters controls) throws JRPCException {
        this(rowType, query, Language.SQL, schema, connection, schedule, controls);
    }

    /**
     * @param rowType    a JavaType into which each row of a delta will be deserialized.
     * @param query      the sql query to be polled
     * @param schema     the default schema for resolving table names in the query.
     * @param connection the connection on which the polled query's notifications will be delivered
     * @param schedule   the schedule for polling the query.  For use with ScheduledQuery and subclasses thereof
     * @param controls   the control parameters to use for polling and reporting.
     * @throws JRPCException if registration on the connection fails
     */
    protected PolledQuery(JavaType rowType, String query, String schema,
                          TriggerwareConnection connection, PolledQuerySchedule schedule, PolledQueryControlParameters controls) throws JRPCException {
        this(rowType, query, Language.SQL, schema, connection, schedule, controls);
    }


    /**
     * @param rowType    the java class into which each row of a delta will be deserialized.
     * @param query      the query to be polled
     * @param language   the language (Language.SQL, Language.FOL) in which the query is written
     * @param namespace  the default schema (package) for resolving table (relation) names in the query.
     * @param connection the connection on which the polled query's notifications will be delivered
     * @param schedule   the schedule for polling the query.  For use with ScheduledQuery and subclasses thereof
     * @param controls   the control parameters to use for polling and reporting.
     * @throws JRPCException if registration on the connection fails
     */
    protected PolledQuery(Class<?> rowType, String query, String language, String namespace,
                          TriggerwareConnection connection, PolledQuerySchedule schedule, PolledQueryControlParameters controls) throws JRPCException {
        super(rowType, query, language, namespace);
        this.preparedQuery = null;
        this.controls = controls;
        this.schedule = schedule;
        register(connection);
    }

    /**
     * @param rowType    a JavaType into which each row of a delta will be deserialized.
     * @param query      the query to be polled
     * @param language   the language (Language.SQL, Language.FOL) in which the query is written
     * @param namespace  the default schema (package) for resolving table (relation) names in the query.
     * @param connection the connection on which the polled query's notifications will be delivered
     * @param schedule   the schedule for polling the query.  For use with ScheduledQuery and subclasses thereof
     * @param controls   the control parameters to use for polling and reporting.
     * @throws JRPCException if registration on the connection fails
     */
    protected PolledQuery(JavaType rowType, String query, String language, String namespace,
                          TriggerwareConnection connection, PolledQuerySchedule schedule, PolledQueryControlParameters controls) throws JRPCException {
        super(rowType, query, language, namespace);
        this.preparedQuery = null;
        this.schedule = schedule;
        this.controls = controls;
        register(connection);
    }

    /**
     * @param pq       A PreparedQuery to use for polling
     * @param schedule the schedule for polling the query.  Should be null if the query will use ad-hoc polling.
     * @param schedule the schedule for polling the query.  For use with ScheduledQuery and subclasses thereof
     * @param controls the control parameters to use for polling and reporting.
     * @throws JRPCException              if registration of the query with the TW server fails
     * @throws TriggerwareClientException if the prepred query is not fully instantiated
     */
    protected PolledQuery(PreparedQuery<T> pq, PolledQuerySchedule schedule, PolledQueryControlParameters controls) throws JRPCException, TriggerwareClientException {
        super(pq);
        this.preparedQuery = pq;
        this.schedule = schedule;
        this.controls = controls;
        if (!preparedQuery.fullyInstantiated())
            throw new TriggerwareClientException(
                    "registering a PolledQuery instance based on a PreparedQuery requires that the PreparedQuery to have values for all of its parameters");
        register();
    }

    private final void setNotificationType() {
        var tf = TypeFactory.defaultInstance();
        if (rowClass != null)
            notificationType = tf.constructParametricType(PolledQueryNotification.class, rowClass);
        else if (rowJType != null)
            notificationType = tf.constructParametricType(PolledQueryNotification.class, rowJType);
        else notificationType = this.rowTypeRef;
    }

    /**
     * getNotificationType required internally.  There is no reason for a TriggerwareClient application to call it.
     */
    @Override
    public Object getNotificationType() {
        return notificationType;
    }

    /**
     * @return the row signature attribute names for this polled query
     */
    public String[] getSignatureNames() {
        //if (!isRegistered) ;
        return signatureNames;
    }

    /**
     * @return the row signature types for this polled query
     */
    public Class<?>[] getSignatureTypes() {
        //if (!isRegistered) ;
        return signatureTypes;
    }


    /**
     * If handleError is not overriden in a subclass, error notifications will be logged but otherwise ignored.
     *
     * @param message text explaining the failure
     * @param ts      The polling time when the failure occurred.
     *                <em>WARNING</em> The timestamp is generated from the TW server machine's clock.
     *                Comparing such a time with timestamps from clock's not synchronized with that server's clock
     *                is prone to error.
     */
    public void handleError(String message, Instant ts) {
        Logging.log("error notification for polled query  %s polled at %s: %s",
                this, ts, message);
    }

    /**
     * handleSuccess must be overriden in  subclasses.
     *
     * @param delta the changes detected by a polling operation
     * @param ts    The polling time when the polling operation was done.
     *              <em>WARNING</em> The timestamp is generated from the TW server machine's clock.
     *              Comparing such a time with timestamps from clock's not synchronized with that server's clock
     *              is prone to error.
     */
    public abstract void handleSuccess(RowsDelta<T> delta, Instant ts);

    /**
     * @return at least one polling operation has succeeded, so the polled query does have a current state.
     * This will be false if called before or during the first execution of handleSuccess for this query, but true thereafter.
     */
    public boolean hasSucceeded() {
        return hasSucceeded;
    }

    String getNotificationTag() {
        return notificationMethod;
    }

    protected synchronized void registered(PolledQueryRegistration pqResult, TriggerwareConnection connection) {
        recordRegistration(connection, pqResult.handle);
        signatureTypes = pqResult.typeSignature();
        signatureTypeNames = pqResult.typeNames();
        signatureNames = pqResult.attributeNames();
        setNotificationType();
        //isRegistered = true;
        //connection.registerPolledQueryHandle(pqResult.handle, this);
    }

    //static PolledQuery<?> fromHandle(int handle, TriggerwareConnection conn) {
    //	return conn.getPolledQueryFromHandle(handle);}

    private void addRequestParamsForControls(NamedRequestParameters parms) {
        if (controls != null) {
            if (controls.pollTimeout != null) parms.with("timelimit", controls.pollTimeout.getSeconds());
            if (controls.reportUnchanged) parms.with("report-noops", true);
            parms.with("report-initial", (controls.reportInitial) ? "with delta" : "without delta");
        } else parms.with("report-initial", "without delta");
    }

    protected NamedRequestParameters getCreateParameters() {
        var parms = new NamedRequestParameters()
                .with("query", query)
                .with("language", language)
                .with("namespace", schema)
                .with("method", notificationMethod);
        addRequestParamsForControls(parms);
        return parms;
    }

    protected NamedRequestParameters getCreateParametersPrepared() {
        var parms = new NamedRequestParameters()
                .with("preparedQueryHandle", preparedQuery.getHandle())
                .with("preparedQueryParameters", preparedQuery.getParameters())
                .with("method", notificationMethod);
        addRequestParamsForControls(parms);
        return parms;
    }

    /**
     * register registers the PolledQuery with the TW server. This polled query must be one constructed
     * using a PreparedQuery as its basis.
     *
     * @throws TriggerwareClientException if this polled query does not has a prepared query as its basis,
     *                                    or if that prepared query does not have all its parameters instantiated.
     * @throws JRPCException              if the TW server rejects the polled query for any reason
     */
    private void register() throws JRPCException, TriggerwareClientException {
        var connection = preparedQuery.getConnection();
        connection.getAgent().registerNotificationInducer(notificationMethod, this);
        try {
            var pqResult = (PolledQueryRegistration) connection.synchronousRPC(PolledQueryRegistration.class, null, null,
                    "create-polled-query", getCreateParametersPrepared());
            registered(pqResult, connection);
        } catch (Throwable t) {
            connection.getAgent().unregisterNotificationInducer(notificationMethod);
            throw t;
        }
    }

    /**register registers the PolledQuery with the TW server. This method is equivalent to calling
     * register(client.getPrimaryConnection())
     * @param client  the polled query's notifications will arrive on this client's primary connection
     * @throws JRPCException if the TW server rejects the polled query for any reason
     */
    //public void register(TriggerwareClient client) throws JRPCException {
    //	register(client.getPrimaryConnection());}

    /**
     * register registers the PolledQuery with the TW server.
     *
     * @param connection the polled query's notifications will arrive on this connection
     * @throws JRPCException if the TW server rejects the polled query for any reason
     */
    private synchronized void register(TriggerwareConnection connection) throws JRPCException {
        if (this.connection != null)
            throw new AbstractQuery.ReregistrationError();
        connection.getAgent().registerNotificationInducer(notificationMethod, this);

        try {
            var pqResult = (PolledQueryRegistration) connection.synchronousRPC(PolledQueryRegistration.class, null, null,
                    "create-polled-query", getCreateParameters());
            registered(pqResult, connection);
        } catch (Throwable t) {
            connection.getAgent().unregisterNotificationInducer(notificationMethod);
            throw t;
        }
    }


    private static final PositionalParameterRequest<Void> pollRequest =
            new PositionalParameterRequest<Void>(Void.TYPE, null, "poll-now", 1, 2);

    /**
     * perform an on-demand poll of this PolledQuery.
     *
     * @throws TriggerwareClientException if this PolledQuery has been closed or has never been registered.
     * @throws JRPCException              for any error (probably communications failure) signalled by the TW server
     *                                    This relates <em>only</em> to errors with understanding and acknowledging the request. Any errors that
     *                                    occur in carrying out the requested poll operation are encoded in an error notification to be handled
     *                                    by the handleError method.
     */
    public synchronized void poll() throws JRPCException, TriggerwareClientException {
        if (closed)
            throw new TriggerwareClientException("attempt to poll a closed PolledQuery");
        if (twHandle == null)
            throw new TriggerwareClientException("attempt to poll an unregistered PolledQuery");
        var timeout = (controls == null) ? null : controls.pollTimeout;
        if (timeout == null) pollRequest.execute(connection, twHandle);
        else pollRequest.execute(connection, twHandle, timeout.toSeconds());
    }

    private static final PositionalParameterRequest<Void> releasePolledQueryRequest =
            new PositionalParameterRequest<Void>(Void.TYPE, null, "close-polled-query", 1, 1);

    /**
     * closeQuery
     * <ul>
     * <li>marks this PolledQuery as closed, so than any future poll requests issued by the client will throw an exception</li>
     * <li>tells the TW Server to release all resources associated with the polled query</li>
     * <li>if this PolledQuery is a ScheduledQuery, the TW server will not initiate any further poll operations on the query</li>
     * </ul>
     * <p>It is possible that the notification queue for the query's connection contains notifications for this query at the
     * time closeQuery is invoked.
     * The PolledQuery's handleSuccess/handleFailure methods will eventually be invoked for such notifications.
     * </p><p>
     * It is even possible (due to race conditions) that further notifications will arrive after closeQuery is invoked.
     * Such notification will be discarded.
     * </p><p>
     * closeQuery is a noop for a PolledQuery that is already closed or one
     * that has never been registered.  A PolledQuery is implicitly unregistered if the query's connection is closed.
     * </p>
     *
     * @return true if the query was successfully closed.  false if it was already closed or if the server was unable to confirm
     * closing it.
     */
    @Override
    public synchronized boolean closeQuery() {
        if (closed) return false;
        try {
            releasePolledQueryRequest.execute(connection, twHandle);
            closed = true;
            connection.getAgent().unregisterNotificationInducer(notificationMethod);
            return true;
        } catch (Exception e) {
            Logging.log("error closing a PolledQuery <%s>", e.getMessage());
            return false;
        }
    }

    /**
     * @return true if closeQuery has been successfully called on this PolledQuery.
     */
    @Override
    public boolean isClosed() {
        return closed;
    }
}
