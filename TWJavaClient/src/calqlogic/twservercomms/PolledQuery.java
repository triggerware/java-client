package calqlogic.twservercomms;

import java.lang.reflect.Constructor;
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
 * the result (a set of 'rows') establishes a 'current state' of the query. For each succeeding execution (referred to
 * as <em>polling</em> the query), </p>
 * <ul>
 * <li> the new result is compared with the current state, and the differences are sent to the triggerware client
 * in a notification containing a {@link RowsDelta} value.
 * <li> the new answer  becomes the current state to be used for comparison with the result of the next poll of the query.
 * </ul>
 * <p> Like any other query, a PolledQuery has a query string, a language (FOL or SQL), and a namespace.
 *</p><p> A polling operation may be performed at any time by executing the {@link #poll()} method.
 *Some details of reporting and polling can be configured with a {@link PolledQuery.PolledQueryControlParameters}
 *value that is supplied to the constructor of a PolledQuery.
 *</p><p>
 *After creating an instance of PolledQuery, it must be 'registered' with its Triggerware server by executing the register method.
 *</p><p>
 *PolledQuery is an abstract class. An instantiable subclass of PolledQuery must provide or inherit a {@link #handleSuccess} method to deal with notifications of 
 *changes to the current state.  There are errors that can occur during a polling operation (timeout, inability to contact
 *a data source).  When such an error occurs, the TW Server will send an "error" notification.
 *An instantiable subclass of PolledQuery may provide a {@link #handleError} method to deal with error notifications.
 *</p><p>
 *{@link ScheduledQuery} is a subclass of PolledQuery which provides for polling to occur based on a schedule, rather than 
 *solely on demand (ie, solely by virtue of the client submitting a poll request to the TW server). The existence of 
 *scheduled queries is the reason that state deltas are sent to the client as notifications, even when a poll is explicitly
 *requested. Scheduled queries may still be polled on demand at any time.
 *</p><p>
 *Polling is terminated when the {@link #close} method is performed.
 *</p><p>
 * If a polling operation is ready to start (whether due to its schedule or an explicit poll request) and a previous poll of
 * the query has not completed, the poll operation that is ready to start is simply skipped, and an error notification is
 * sent to the client.
 *</p>
 * @param <T> the class that repesents a single 'row' of the answer to the query.
 * @see ScheduledQuery
 */
public abstract class PolledQuery<T> extends AbstractQuery<T> {
	protected final String notificationMethod = TriggerwareClient.nextNotificationMethod("pq");
	protected  Class<?>[] signatureTypes;  
	protected  String[] signatureNames, signatureTypeNames;
	//protected final PolledQuerySchedule xschedule;
	protected final PolledQueryControlParameters controls;
	//private  JavaType notificationType; // set when the TriggerwareClient is known
	protected boolean hasSucceeded = false;
	//protected final PreparedQuery<T> preparedQuery;
	protected SignatureElement[]outputSignature = null;

	/**
	 *PolledQueryControlParameters contains values that give the client some control over the reporting of the changes
	 *to the query.
	 *
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
		 * the initial success notification from other success notifications is to use the hasSucceeded method.
		 * The default value is false,  meaning <em>not</em> to include rows in that notification.
		 */
		public final boolean reportInitial; //should the initial delta be reported with the initial polling notification
		/*private PolledQueryControlParameters() {
			reportUnchanged = false;
			pollTimeout = null;
			reportInitial = false;
		}*/
		/**
		 * Construct a PolledQueryControlParameters instance providing a value for each parameter.
		 * @param reportUnchanged the value for the reportUnchanged parameter
		 * @param reportInitial the value for the reportInitial parameter
		 * @param pollTimeout the value for the pollTimeout parameter
		 */
		public PolledQueryControlParameters(boolean reportUnchanged, boolean reportInitial, Duration pollTimeout) {
			this.reportUnchanged = reportUnchanged;
			this.pollTimeout = pollTimeout;
			this.reportInitial = reportInitial;
		}
	}
	
	/**
	 * defaultControlParameters is a PolledQueryControlParameters instance with no initial rows being reported, no timeout, 
	 * and no delta reported when there is not change to the rows
	 */
	public static PolledQueryControlParameters defaultControlParameters = new PolledQueryControlParameters(false, false, null);

	@JsonFormat(shape=JsonFormat.Shape.OBJECT)
	static class PolledQueryRegistration {//the result of registering a polled query
		final int handle;
		final SignatureElement[]signature;

		@JsonCreator
		PolledQueryRegistration(@JsonProperty("handle")int handle, @JsonProperty("signature")SignatureElement[] signature){
			this.handle = handle;
			this.signature = signature;		
		}

		Class<?>[] typeSignature(){return AbstractQuery.typeSignatureTypes(signature);}
		String[] typeNames(){return typeSignatureTypeNames(signature);}
		String[] attributeNames() {return signatureNames(signature);}
	}
	
	/**Create a polled query using SQL syntax and register it for use on the primary connection of a client.
	 * @param client the client on whose primary connection the polled query's notifications will be delivered
	 * @param rowClass the java class into which each row of a delta will be deserialized.
	 * @param query the sql query to be polled
	 * @param schema  the default schema for resolving table names in the query.
	 * @param controls the control parameters to use for polling and reporting.
	 * @throws JRPCException if registration on the connection fails
	 */
	protected  PolledQuery(TriggerwareClient client, Class<T>rowClass, String query,  String schema,
			     PolledQueryControlParameters controls) throws JRPCException {
		this(client.getPrimaryConnection(), rowClass, query,  Language.SQL, schema, controls);}
	
	/**Create a polled query using SQL syntax and register it for use on the primary connection of a client.
	 * @param client the client on whose primary connection the polled query's notifications will be delivered
	 * @param rowClass the java class into which each row of a delta will be deserialized.
	 * @param query the sql query to be polled
	 * @param schema  the default schema for resolving table names in the query.
	 * @param controls the control parameters to use for polling and reporting.
	 * @throws JRPCException if registration on the connection fails
	 */
	protected  PolledQuery(TriggerwareClient client, Class<T>rowClass, String query,  String schema,
			 PolledQuerySchedule schedule, PolledQueryControlParameters controls) throws JRPCException {
		this(client.getPrimaryConnection(), rowClass, query,  Language.SQL, schema,  /*schedule,*/ controls);}
	
	/**
	 * Create a polled query using SQL syntax and register it for use on a connection.
	 * @param connection the connection on which the polled query's notifications will be delivered
	 * @param rowClass a Java class into which each row of a delta will be deserialized.
	 * @param query the sql query to be polled
	 * @param schema  the default schema for resolving table names in the query.
	 * @param controls the control parameters to use for polling and reporting.
	 * @throws JRPCException if registration on the connection fails
	 */
	protected  PolledQuery(TriggerwareConnection connection, Class<T> rowClass, String query,  String schema,
			 /*PolledQuerySchedule schedule,*/ PolledQueryControlParameters controls) throws JRPCException {
		this(connection, rowClass, query,  Language.SQL, schema,  /*schedule,*/ controls);}
	
	
	/**
	 * Create a polled query 
	 * This is the only PolledQuery constructor that directly creates a PolledQuery,
	 * Other constructors simply provided default values for one or more parameters of this constructor.
	 * @param rowClass the java class into which each row of a delta will be deserialized.
	 * @param query the query to be polled
	 * @param language  the language (Language.SQL, Language.FOL) in which the query is written
	 * @param schema  the default schema  for resolving table  names in the query.
	 * @param connection the connection on which the polled query's notifications will be delivered
	 * @param controls the control parameters to use for polling and reporting.
	 * @throws JRPCException if registration on the connection fails
	 */
	protected  PolledQuery(TriggerwareConnection connection, Class<T>rowClass, String query, String language, String schema,
			 /*PolledQuerySchedule schedule,*/ PolledQueryControlParameters controls) throws JRPCException {
		super(rowClass, query, language, schema);
		//preparedQuery = null;
		this.controls = controls;
		//this.xschedule = schedule;
		this.connection = connection;
		//notificationType = TypeFactory.defaultInstance().constructParametricType(PolledQueryNotification.class, rowClass);
		//register();
	}
	/**
	 * @param rowClass a JavaType into which each row of a delta will be deserialized.
	 * @param query the query to be polled
	 * @param language  the language (Language.SQL, Language.FOL) in which the query is written
	 * @param namespace  the default schema (package) for resolving table (relation) names in the query.
	 * @param connection the connection on which the polled query's notifications will be delivered
	 * @param schedule the schedule for polling the query.  For use with ScheduledQuery and subclasses thereof
	 * @param controls the control parameters to use for polling and reporting.
	 * @throws JRPCException if registration on the connection fails
	 */
	/*protected  PolledQuery(JavaType rowClass, String query, String language, String namespace,
			TriggerwareConnection connection,  PolledQuerySchedule schedule, PolledQueryControlParameters controls) throws JRPCException {
		super(rowClass, query, language, namespace);
		this.preparedQuery = null;
		this.schedule = schedule;
		this.controls = controls;
		register(connection);
	}*/
	
	/**
	 * @param pq  A PreparedQuery to use for polling
	 * @param schedule the schedule for polling the query.  Should be null if the query will use ad-hoc polling.
	 * @param schedule the schedule for polling the query.  For use with ScheduledQuery and subclasses thereof
	 * @param controls the control parameters to use for polling and reporting.
	 * @throws JRPCException if registration of the query with the TW server fails
	 * @throws TriggerwareClientException if the prepred query is not fully instantiated
	 */
	/*protected PolledQuery(PreparedQuery<T> pq, PolledQuerySchedule schedule, PolledQueryControlParameters controls) throws JRPCException, TriggerwareClientException {
		super(pq);
		this.preparedQuery = pq;
		this.schedule = schedule;
		this.controls = controls;
		notificationType = TypeFactory.defaultInstance().constructParametricType(PolledQueryNotification.class, rowClass);

		if (preparedQuery == null || !preparedQuery.fullyInstantiated())
			throw new TriggerwareClientException(
				"registering a PolledQuery instance based on a PreparedQuery requires a non-null PreparedQuery with values for all of its parameters");
		register();
	}*/
	
	/**
	 * getNotificationType required internally.  There is no reason for a TriggerwareClient application to call it.
	 */
	//@Override
	//public Object getNotificationType() {return notificationType;}
	
	/**
	 * @return the row signature attribute names for this polled query
	 */
	public String[] getSignatureNames(){
		return signatureNames;}
	
	/**
	 * @return the row signature types for this polled query
	 */
	public Class<?>[] getSignatureTypes(){
		return signatureTypes;}
	

	/**
	 * If handleError is not overriden in a subclass, error notifications will be logged but otherwise ignored. 
	 * @param message text explaining the failure
	 * @param timeStamp The polling time when the failure occurred.
	 * <em>WARNING</em> The timestamp is generated from the TW server machine's clock.
	 * Comparing such a time with timestamps from clocks not synchronized with that server's clock
	 * is prone to error.
	 */
	public void handleError(String message, Instant timeStamp) {
		Logging.log("error notification for polled query  %s polled at %s: %s", this, timeStamp, message);}
	
	/**
	 * handleSuccess must be overriden in  subclasses.
	 * @param delta the changes detected by a polling operation
	 * @param timeStamp The polling time when the polling operation was done.
	 * <em>WARNING</em> The timestamp is generated from the TW server machine's clock.
	 * Comparing such a time with timestamps from clocks not synchronized with that server's clock
	 * is prone to error.
	 */
	public abstract void handleSuccess(RowsDelta<T> delta, Instant timeStamp);
	
	/**
	 * @return at least one polling operation has succeeded, so the polled query does have a current state on the TW server.
	 * This will be false if called before or during the first execution of handleSuccess for this query, but true thereafter.
	 */
	public boolean hasSucceeded() {return hasSucceeded;}
	
	static class PolledQueryRequest<T> extends NamedParameterRequest<PolledQueryRegistration>{
		//private final Constructor<T> rowConstructor;
		//private SignatureElement[] signature;
		private final static String[]requiredParams = new String[] {"language", "query", "namespace"},
	               			   optionalParams = new String[] {"method","schedule", "report-noops", "delay-schedule", "report-initial","limit", "timelimit"};

		PolledQueryRequest(/*Class<T>rowClass,*/ Constructor<T> rowConstructor){
			super(PolledQueryRegistration.class, "create-polled-query",  requiredParams, optionalParams);
		}
	}
	static class PolledQueryNotificationInducer<T> implements NotificationInducer{
		private final SignatureElement[] signature;
		private final Constructor<T> rowConstructor;
		private final JavaType notificationType;
		private final PolledQuery<T> pq;
		PolledQueryNotificationInducer(Class<T> rowClass, PolledQuery<T> pq, SignatureElement[] signature, Constructor<T> rowConstructor){
			super();
			this.pq = pq;
			this.signature = signature;
			this.rowConstructor = rowConstructor;
			this.notificationType = TypeFactory.defaultInstance().constructParametricType(PolledQueryNotification.class, rowClass);
		}
		PolledQuery<T> getQuery() {return pq;}
		@Override
		public void establishDeserializationAttributes(SerializationState ss) {
			ss.put("rowSignature", signature);
			if (rowConstructor != null)
				ss.put("rowBeanConstructor", rowConstructor);
		};

		@Override
		public JavaType getNotificationType() {
			return notificationType;}
	}
	/**register registers the PolledQuery with the TW server. 

	 * @throws JRPCException if the TW server rejects the polled query for any reason
	 */
	public synchronized void register()throws JRPCException {
		//var connection = preparedQuery.getConnection();
		var cpqReq = new PolledQueryRequest<T>(/*rowClass,*/ rowConstructor);
		var params = getCreateParameters();//preparedQuery == null ? getCreateParameters() : getCreateParametersPrepared();
		try {
			var pqResult = //(PolledQueryRegistration)connection.synchronousRPC(PolledQueryRegistration.class, "create-polled-query", params);
					(PolledQueryRegistration)connection.synchronousRPC(cpqReq, params);
			//cpqReq.setSignature(pqResult.signature);
			registered(pqResult, connection);
		}catch(Throwable t) {
			connection.getAgent().unregisterNotificationInducer(notificationMethod);
			throw t;
		}
	}

	protected synchronized void registered(PolledQueryRegistration pqResult, TriggerwareConnection connection){
		recordRegistration(connection, pqResult.handle);
		signatureTypes = pqResult.typeSignature();
		signatureTypeNames = pqResult.typeNames();
		signatureNames = pqResult.attributeNames();
		connection.getAgent().registerNotificationInducer(notificationMethod, 
			new PolledQueryNotificationInducer<T>(rowClass, this, pqResult.signature, rowConstructor));
	}
	
	//static PolledQuery<?> fromHandle(int handle, TriggerwareConnection conn) {
	//	return conn.getPolledQueryFromHandle(handle);}

	private void addRequestParamsForControls(NamedRequestParameters parms) {
		if (controls != null) {
			if (controls.pollTimeout != null) parms.with("timelimit", controls.pollTimeout.getSeconds());
			if (controls.reportUnchanged) parms.with("report-noops", true);
			parms.with("report-initial", (controls.reportInitial) ? "with delta" : "without delta");
		}
		else parms.with("report-initial", "without delta");
	}

	protected NamedRequestParameters getCreateParameters() {
		var parms = new NamedRequestParameters().with("query", query).with("language", language).with("namespace", schema)
					.with("method", notificationMethod);
		addRequestParamsForControls(parms);
		return parms;
	}
	
	/*protected NamedRequestParameters getCreateParametersPrepared() {
		var parms = new NamedRequestParameters().with("preparedQueryHandle", preparedQuery.getHandle())
				.with("preparedQueryParameters", preparedQuery.getParameters()).with("method", notificationMethod);
		addRequestParamsForControls(parms);
		return parms;
	}*/

	/**register registers the PolledQuery with the TW server.
	 * @param connection  the polled query's notifications will arrive on this connection
	 * @throws JRPCException if the TW server rejects the polled query for any reason
	 */
	/*private synchronized void register() throws JRPCException {
		//if (this.connection != null)
		//	throw new AbstractQuery.ReregistrationError();
		connection.getAgent().registerNotificationInducer(notificationMethod, this);

		try {
			var pqResult = (PolledQueryRegistration)connection.synchronousRPC(PolledQueryRegistration.class, null, 
							"create-polled-query", getCreateParameters());
			registered(pqResult, connection);
		}catch(Throwable t) {
			connection.getAgent().unregisterNotificationInducer(notificationMethod);
			throw t;
		}
	}*/

	private static PositionalParameterRequest<Void> pollRequest = 
			new PositionalParameterRequest<Void>(Void.TYPE, "poll-now", 1, 2);

	/**
	 * perform an on-demand poll of this PolledQuery.
	 * @throws TriggerwareClientException if this PolledQuery has been closed or has never been registered.
	 * @throws JRPCException for any error (probably communications failure) signalled by the TW server
	 * This relates <em>only</em> to errors with understanding and acknowledging the request. Any errors that
	 * occur in carrying out the requested poll operation are encoded in an error notification to be handled
	 * by the handleError method.
	 */
	public synchronized void poll() throws JRPCException, TriggerwareClientException {
		if (closed)
			throw new TriggerwareClientException("attempt to poll a closed PolledQuery");
		if (twHandle == null)
			throw new TriggerwareClientException("attempt to poll an unregistered PolledQuery");
		var timeout = (controls == null)? null : controls.pollTimeout;
		if (timeout == null) pollRequest.execute(connection, twHandle);
		else pollRequest.execute(connection, twHandle, timeout.toSeconds());
	}
	
	private static PositionalParameterRequest<Void> releasePolledQueryRequest =
			new PositionalParameterRequest<Void>(Void.TYPE, "close-polled-query", 1, 1);
			
	/**
	 *close 
	 *<ul>
	 *<li>marks this PolledQuery as closed, so than any future poll requests issued by the client will throw an exception</li>
	 *<li>tells the TW Server to release all resources associated with the polled query</li>
	 *<li>if this PolledQuery is a ScheduledQuery, the TW server will not initiate any further poll operations on the query</li>
	 *</ul>
	 *<p>It is possible that the notification queue for the query's connection contains notifications for this query at the
	 *time close is invoked.
	 *The PolledQuery's handleSuccess/handleFailure methods will eventually be invoked for such notifications.
	 *</p><p> 
	 *It is even possible (due to race conditions) that further notifications will arrive after close is invoked.
	 *Such notification will be discarded.
	 *</p><p> 
	 *close is a noop for a PolledQuery that is already closed or one
	 *that has never been registered.  A PolledQuery is implicitly unregistered if the query's connection is closed.
	 *</p>
	 */
	@Override
	public synchronized void close() {
		if (closed) return;// false;
		try {
			releasePolledQueryRequest.execute(connection, twHandle);
			closed = true;
			connection.getAgent().unregisterNotificationInducer(notificationMethod);
			//return true;
		} catch (Exception e) {
		   Logging.log("error closing a PolledQuery <%s>", e.getMessage());
		   //return false;
		}		
	}
}
