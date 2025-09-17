package nmg.softwareworks.jrpcagent;


/**
 * A NotificationInducer&lt;T&gt; ni is required to call
*  client.registerNotificationInducer(methodName, ni)
*  where client is the JRPCAgent receiving notifications using methodName as the method property of the notification.
*  This happens in the method that "registers" the inducer with the tw server
*  and allows the deserializer to find the inducer, and then (from getNotificationType) 
*  the target type for deserializing the notification
 */
public interface NotificationInducer {
	/**
	 * @return the jackson 'type' (a class, or a JavaType) to use for deserializing notifications for this
	 * inducer.  This must designate a type that inherits from Notification&lt;T&gt;
	 */
	Object getNotificationType();
	//default boolean isClosed() {return false;}
	void establishDeserializationAttributes(SerializationState ss);
}
