package calqlogic.twservercomms;

import java.io.*;
import java.net.Socket;
import java.util.HashSet;
import com.fasterxml.jackson.databind.module.SimpleModule;
import nmg.softwareworks.jrpcagent.Connection;

/**
 * a connection to a Triggerware server.  All requests are sent on a TriggerwareConnection.
 * Responses and notifications are received on a TriggerwareConnection.
 * Closing a TriggerwareConnection will release all resources on its  Triggerware server that are associated with the connection.
 */
public class TriggerwareConnection extends Connection{

	private String defaultSchema = null;
	private HashSet<PreparedQuery<?>>myPreparedQueries = new HashSet<>();
	boolean addPreparedQuery(PreparedQuery<?> pq){
		return myPreparedQueries.add(pq);}
	boolean removePreparedQuery(PreparedQuery<?> pq) {
		return myPreparedQueries.remove(pq);}
	

	/*private HashSet<View<?>>myViews = new HashSet<>();
	boolean addView(View<?> v){
		return myViews.add(v);}
	boolean removeView(View<?> v) {
		return myViews.remove(v);}*/

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
		this(twClient, sock.getInputStream(), sock.getOutputStream());		}

	TriggerwareConnection(TriggerwareClient twClient, InputStream istream, OutputStream ostream) throws IOException {
		super(twClient, istream, ostream);
		this.twClient = twClient;
		defaultSchema = twClient.getDefaultSchema();
		var sm = new SimpleModule();
		sm.addDeserializer(BatchNotification.class, new BatchNotification.BatchNotificationDeserializer(this));
		this.getPartnerMapper().registerModule(sm);
	}

	/**
	 * @return the client using this connection
	 */
	public TriggerwareClient getClient() {return twClient;}

	@Override
	protected void onDisconnect() {
		myPreparedQueries.clear();
		//myPolledQueries.clear();
		twClient.onTWCommunicationsLost(this);}

}
