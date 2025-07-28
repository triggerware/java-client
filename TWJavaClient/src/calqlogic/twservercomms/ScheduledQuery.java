package calqlogic.twservercomms;


import com.fasterxml.jackson.databind.JavaType;
import calqlogic.twservercomms.TriggerwareClient.TriggerwareClientException;
import nmg.softwareworks.jrpcagent.JRPCException;
import nmg.softwareworks.jrpcagent.NamedRequestParameters;
import nmg.softwareworks.jrpcagent.PositionalParameterRequest;

/**
 * A ScheduledQuery is a PolledQuery with a schedule for conducting the polling operations
 *
 * @param <T> The type which implements an instance of a row that can be reported in a change notification
 */
public abstract class ScheduledQuery<T> extends PolledQuery<T> {
	//private final PolledQuerySchedule schedule;
	private boolean active = false, paused = false;
	/**
	 * @param rowType the java class into which each row of a delta will be deserialized.
	 * @param query the query to be polled
	 * @param schema  the default schema for resolving table (relation) names in the query.
	 * @param connection  the connection on which the changes will be reported
	 * @param controls the control parameters to use for polling and reporting.
	 * @param schedule the schedule for polling the query
	 * @throws JRPCException if registration on the connection fails
	 */
	public ScheduledQuery(Class<?>rowType, PolledQuerySchedule schedule, 	String query,  String schema,
			TriggerwareConnection connection, PolledQueryControlParameters controls) throws JRPCException{
		this(rowType, schedule, query, Language.SQL, schema,connection, controls);
	}
	
	/**
	 * @param rowType the java class into which each row of a delta will be deserialized.
	 * @param query the query to be polled
	 * @param language  the language (Language.SQL, Language.FOL) in which the query is written
	 * @param schema  the default schema (package) for resolving table (relation) names in the query.
	 * @param connection  the connection on which the changes will be reported
	 * @param controls the control parameters to use for polling and reporting.
	 * @param schedule the schedule for polling the query
	 * @throws JRPCException if registration on the connection fails
	 */
	public ScheduledQuery(Class<?>rowType, PolledQuerySchedule schedule, 	String query, String language, String schema,
			TriggerwareConnection connection, PolledQueryControlParameters controls) throws JRPCException{
		super(rowType, query, language, schema,connection, schedule, controls);
		//this.schedule = schedule;
	}
	/**
	 * @param rowType the JavaType into which each row of a delta will be deserialized.
	 * @param query the query to be polled
	 * @param language  the language (Language.SQL, Language.FOL) in which the query is written
	 * @param schema  the default schema (package) for resolving table (relation) names in the query.
	 * @param connection  the connection on which the changes will be reported
	 * @param controls the control parameters to use for polling and reporting.
	 * @param schedule the schedule for polling the query
	 * @throws JRPCException if registration on the connection fails
	 */
	public ScheduledQuery(JavaType rowType, PolledQuerySchedule schedule, String query, String language, String schema, 
					TriggerwareConnection connection, PolledQueryControlParameters controls) throws JRPCException{
		super(rowType, query, language, schema, connection, schedule, controls);
		//this.schedule = schedule;
	}
	
	/**
	 * @return the schedule controlling this ScheduledQuery
	 */
	public PolledQuerySchedule getSchedule(){return schedule;}
	
	@Override
	protected NamedRequestParameters getCreateParameters() {
		return super.getCreateParameters().with("schedule", schedule).with("delay-schedule", true);	}
	
	@Override
	protected NamedRequestParameters getCreateParametersPrepared() {
		return getCreateParametersPrepared().with("schedule", schedule).with("delay-schedule", true);}
	
	@SuppressWarnings("unused")
	private boolean hasBeenPolled = false; // does not seem useful under current tw protocol
	@Override
	public synchronized void poll() throws JRPCException, TriggerwareClientException {
		try {super.poll();
		}finally {hasBeenPolled = true;}
	}
	
	//private static PositionalParameterRequest<Void> activateQueryRequest = 
	//		new PositionalParameterRequest<Void>(Void.TYPE,  "activate-scheduled-polled-query", 1, 2);
	/**
	 * activate this ScheduledQuery on the TW server. 
	 * It will be polled immediately to establish a current state,
	 * It will polled at all future times dictated by its schedule. The per-schedule polling will occur even if the
	 * immediate poll fails.  If that is not acceptable, the application is responsible for deciding how to deal with the
	 * problem in its {@link handleError} method -- e.g., do deactivate this ScheduledQuery, or attempt further on-demand polling.
	 * @throws JRPCException -- for any error signalled by the TW server in response to the requested initial poll. 
	 * @throws TriggerwareClientException if this ScheduledQuery is already active, or is closed, or has never been registered.
	 */
	public synchronized void activate(/*OffsetDateTime when*/) throws JRPCException, TriggerwareClientException  {
		if (closed)
			throw new TriggerwareClientException("attempt to activate a closed ScheduledQuery");
		if (active) 
			throw new TriggerwareClientException("attempt to activate an active ScheduledQuery");
		if (twHandle == null)
			throw new TriggerwareClientException("attempt to activate an unregistered ScheduledQuery");
		/*if (when == null)
			activateQueryRequest.execute(connection, twHandle);
		else activateQueryRequest.execute(connection, twHandle, JsonTimeUtilities.asJson(when));*/
		poll();
		active = true;
	}
	
	/*public*/synchronized void pause() throws JRPCException, TriggerwareClientException  {
		if (closed)
			throw new TriggerwareClientException("attempt to pause a closed ScheduledQuery");
		if(paused) 
			throw new TriggerwareClientException("request to pause a scheduled query that is already paused");
		if (!active) 
			throw new TriggerwareClientException("request to pause a scheduled query that is not active");
		//
		paused = true;
	}
	/*public*/ synchronized void resume() throws JRPCException, TriggerwareClientException  {
		if (closed)
			throw new TriggerwareClientException("attempt to resume a closed ScheduledQuery");
		if(!paused) 
			throw new TriggerwareClientException("request to resume a scheduled query that is not paused");
		if (!active) 
			throw new TriggerwareClientException("request to pause a scheduled query that is not active");
		paused = false;
	}

	/**
	* deactivate is a synonym for closeQuery.
	* There is no means to quit polling the ScheduledQuery according to its schedule, yet <em>retain</em> the option of
	* doing future on-demand polling of the query.
	*/
	public synchronized void deactivate() {
		super.closeQuery();	}
}
