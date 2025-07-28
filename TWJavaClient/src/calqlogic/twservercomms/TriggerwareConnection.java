package calqlogic.twservercomms;

import java.io.*;
import java.net.Socket;
import java.util.HashSet;
import nmg.softwareworks.jrpcagent.Connection;

public class TriggerwareConnection extends Connection{
	
	
	/*private HashMap<Integer, PolledQuery<?>> myPolledQueries = new HashMap<>();
	PolledQuery<?> getPolledQueryFromHandle(int handle) {
		return myPolledQueries.get(handle);	}
	PolledQuery<?> registerPolledQueryHandle(int handle, PolledQuery<?>pq){
		return myPolledQueries.put(handle, pq);}
	PolledQuery<?> unregisterPolledQueryHandle(int handle) {
		return myPolledQueries.remove(handle);}*/
	
	/*private HashMap<Integer,PushResultController<?>> myQueryResultControllers = new HashMap<>();
	PushResultController<?> getQueryResultControllerFromHandle(int handle) {
		return myQueryResultControllers.get(handle);	}
	PushResultController<?> registerQueryResultController(int handle, PushResultController<?>rc){
		return myQueryResultControllers.put(handle, rc);}
	PushResultController<?> unregisterQueryResultController(int handle) {
		return myQueryResultControllers.remove(handle);}*/
	private String defaultSchema = null;
	private final HashSet<PreparedQuery<?>>myPreparedQueries = new HashSet<>();
	boolean addPreparedQuery(PreparedQuery<?> pq){
		return myPreparedQueries.add(pq);}
	boolean removePreparedQuery(PreparedQuery<?> pq) {
		return myPreparedQueries.remove(pq);}
	

	private final HashSet<View<?>>myViews = new HashSet<>();
	boolean addView(View<?> v){
		return myViews.add(v);}
	boolean removeView(View<?> v) {
		return myViews.remove(v);}

	private final TriggerwareClient twClient;
	@Override
	public TriggerwareClient getAgent() {return twClient;}
	
	/**
	 * @return the default schema that will be used in requests that contain SQL text when no schema parameter is provided
	 */
	String getDefaultSchema() {return defaultSchema;}
	
	/**
	 * @param defaultSchema the default schema to use in requests that contain SQL text
	 */
	void setDefaultSchema(String defaultSchema) {this.defaultSchema = defaultSchema;}

	TriggerwareConnection(TriggerwareClient twClient, Socket sock) throws IOException {
		this(twClient, sock.getInputStream(), sock.getOutputStream());}

	TriggerwareConnection(TriggerwareClient twClient, InputStream istream, OutputStream ostream) throws IOException {
		super(twClient, ostream, istream);
		this.twClient = twClient;
		defaultSchema = twClient.getDefaultSchema();
		this.getObjectMapper().registerModule(new BatchNotification.DeserializationModule(this));
	}

	public TriggerwareClient getClient() {return twClient;}
	

	
	@Override
	protected void onDisconnect() {
		myPreparedQueries.clear();
		//myPolledQueries.clear();
		twClient.onTWCommunicationsLost(this);}

}
