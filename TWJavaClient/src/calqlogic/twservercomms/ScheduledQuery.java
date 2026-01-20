package calqlogic.twservercomms;

import calqlogic.twservercomms.TriggerwareClient.TriggerwareClientException;
import nmg.softwareworks.jrpcagent.JRPCException;
import nmg.softwareworks.jrpcagent.NamedRequestParameters;

/**
 * A ScheduledQuery is a PolledQuery with a schedule for conducting the polling operations.
 * This is an abstract class that must be subclassed for use in an application.
 * @param <T> The type which will be used to represent  a row that can be reported in a change notification
 */
public abstract class ScheduledQuery<T> extends PolledQuery<T> {
	private final PolledQuerySchedule schedule; //although the schedule is saved in this field, it is actually only used on the server
	private boolean active = false;//, paused = false;
	/**
	 * create a ScheduledQuery using SQL syntax and register it to report changes on a client's primary connection.
	 * @param client  the client on whose primary connection the changes will be reported
	 * @param rowClass the java class into which each row of a delta will be deserialized.
	 * @param query the query to be polled
	 * @param schema  the default schema for resolving table (relation) names in the query.
	 * @param schedule the schedule for polling the query
	 * @param controls the control parameters to use for polling and reporting.
	 * @throws JRPCException if registration on the connection fails
	 */
	protected ScheduledQuery(TriggerwareClient client, Class<T>rowClass, String query,  String schema, PolledQuerySchedule schedule,
			PolledQueryControlParameters controls) throws JRPCException{
		this(client.getPrimaryConnection(), rowClass,  query, Language.SQL, schema, schedule, controls, null);	}
	
	/**
	 * create a ScheduledQuery and register it to report changes on a client's primary connection.
	 * @param client  the client on whose primary connection the changes will be reported
	 * @param rowClass the java class into which each row of a delta will be deserialized.
	 * @param query the query to be polled
	 * @param language  the language (Language.SQL, Language.FOL) in which the query is written
	 * @param schema  the default schema for resolving table (relation) names in the query.
	 * @param schedule the schedule for polling the query
	 * @param controls the control parameters to use for polling and reporting.
	 * @throws JRPCException if registration on the connection fails
	 */
	protected ScheduledQuery(TriggerwareClient client, Class<T>rowClass, String query, String language, String schema, PolledQuerySchedule schedule,
			PolledQueryControlParameters controls) throws JRPCException{
		this(client.getPrimaryConnection(), rowClass,  query, language, schema, schedule, controls, null);	}
	
	/*
	 * create a ScheduledQuery and register it to report changes on a connection.
	 * other constructors for ScheduledQuery simply provide default values for some of the paramters of this one
	 * @param connection  the connection on which the changes will be reported
	 * @param rowClass the java class into which each row of a delta will be deserialized.
	 * @param query the query to be polled
	 * @param language  the language (Language.SQL, Language.FOL) in which the query is written
	 * @param schema  the default schema  for resolving table  names in the query.
	 * @param schedule the schedule for polling the query
	 * @param controls the control parameters to used for polling and reporting.
	 * @throws JRPCException if registration on the connection fails
	 */
	protected ScheduledQuery(TriggerwareConnection connection, Class<T>rowClass,  	String query, String language, String schema,
			PolledQuerySchedule schedule, PolledQueryControlParameters controls, Double clockFactor) throws JRPCException{
		super(connection, rowClass, query, language, schema, controls);
		
		this.schedule = schedule; 
	}
	/**
	 * @param rowClass the JavaType into which each row of a delta will be deserialized.
	 * @param query the query to be polled
	 * @param language  the language (Language.SQL, Language.FOL) in which the query is written
	 * @param schema  the default schema (package) for resolving table (relation) names in the query.
	 * @param connection  the connection on which the changes will be reported
	 * @param controls the control parameters to use for polling and reporting.
	 * @param schedule the schedule for polling the query
	 * @throws JRPCException if registration on the connection fails
	 */
	/*protected ScheduledQuery(JavaType rowClass, PolledQuerySchedule schedule, String query, String language, String schema, 
					TriggerwareConnection connection, PolledQueryControlParameters controls) throws JRPCException{
		super(rowClass, query, language, schema, connection, schedule, controls);
		//this.schedule = schedule;
	}*/
	
	/**
	 * @return the schedule controlling this ScheduledQuery
	 */
	public PolledQuerySchedule getSchedule(){return schedule;}
	
	@Override
	protected NamedRequestParameters getCreateParameters() {
		return super.getCreateParameters().with("schedule", schedule.asJson()).with("delay-schedule", true);	}
	
	/*@Override
	protected NamedRequestParameters getCreateParametersPrepared() {
		return super.getCreateParametersPrepared().with("schedule", schedule).with("delay-schedule", true);}*/
	
	@SuppressWarnings("unused")
	private boolean hasBeenPolled = false; // does not seem useful under current tw protocol
	@Override
	public synchronized void poll() throws JRPCException, TriggerwareClientException {
		try {super.poll();
		}finally {hasBeenPolled = true;}
	}
	
	/**
	 * activate this ScheduledQuery on the TW server. 
	 * It will be polled immediately to establish a current state,
	 * It will polled at all future times dictated by its schedule. The per-schedule polling will occur even if the
	 * immediate poll fails.  If that is not acceptable, the application is responsible for deciding how to deal with the
	 * problem in its {@link handleError} method -- e.g., deactivate this ScheduledQuery, or attempt further on-demand polling.
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
		poll();
		active = true;
	}
	
	/*synchronized void pause() throws JRPCException, TriggerwareClientException  {
		if (closed)
			throw new TriggerwareClientException("attempt to pause a closed ScheduledQuery");
		if(paused) 
			throw new TriggerwareClientException("request to pause a scheduled query that is already paused");
		if (!active) 
			throw new TriggerwareClientException("request to pause a scheduled query that is not active");
		//
		paused = true;
	}
	synchronized void resume() throws JRPCException, TriggerwareClientException  {
		if (closed)
			throw new TriggerwareClientException("attempt to resume a closed ScheduledQuery");
		if(!paused) 
			throw new TriggerwareClientException("request to resume a scheduled query that is not paused");
		if (!active) 
			throw new TriggerwareClientException("request to pause a scheduled query that is not active");
		paused = false;
	}*/

	/**
	 * @return true if this query has been activated and not yet closed -- i.e., if the query is still being sampled on its schedule
	 */
	public boolean isActive() {return active;}
	@Override
	public void close() {
		super.close();
		active = false;
	}

}
