package nmg.softwareworks.jrpcagent;

/**
 * an interface that must be implemented by any class that is the target of deserialization for notifications
 * sent to a JRPCAgent for a NotificationInducer&lt;T&gt;
 * Instances of the Notification class (which is determined by the NotificationInducer) are created by
 * deserializing the params field of the jrpc notification message.
 */
public abstract class Notification {
	private NotificationInducer inducer;
	/** A method to handle a notification of this class.
	 * @param conn  The connection on which the notification arrived.
	 * @param methodName The method name used in the json notification to be deserialized
	 */
	public abstract void handle(Connection conn, String methodName);
	public NotificationInducer getInducer() {return inducer;}
	//setInducer is only to be called by incomingmessage
	void setInducer(NotificationInducer inducer) { this.inducer = inducer;}
}
