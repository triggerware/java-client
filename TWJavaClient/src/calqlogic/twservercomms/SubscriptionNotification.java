package calqlogic.twservercomms;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import nmg.softwareworks.jrpcagent.Connection;
import nmg.softwareworks.jrpcagent.Logging;
import nmg.softwareworks.jrpcagent.Notification;

/**
 * A SubscriptionNotification represents a notification sent by the TW server due to some active subscription.
 * @author nmg
 *
 * @param <T>  the type of tuple for any instance of this notification.  This is the tuple type of the Subscription 
 * that produced the notification.
 */
@JsonIgnoreProperties(ignoreUnknown = true) //just ignore label and update#
final class SubscriptionNotification <T> extends Notification{
	//private final TriggerwareConnection connection;
	@JsonIgnore
	private Subscription<T> subscription;
	@JsonProperty("tuple")
	private T tuple;

	public SubscriptionNotification() {} //for jackson deserialization
	
	//this constructor is for use from the deserializer for BatchNotifications
	@SuppressWarnings("unchecked")
	SubscriptionNotification(Subscription<?> subscription, Object tuple){
		this.tuple = (T)tuple;
		this.subscription = (Subscription<T>) subscription;		
	}
	/**
	 * @return the Subscription this notification responds to.
	 */
	public Subscription<T> getSubscription() {return subscription;}

	/**
	 * @return the tuple instance of this notification.
	 */
	public T getTuple() {return tuple;}
	
	@Override
	public void handle(Connection conn, String notificationTag) {
		@SuppressWarnings("unchecked")
		var sub = (Subscription<T>) conn.getAgent().getNotificationInducer(notificationTag);
		if (sub == null) {
			Logging.log(String.format("internal error: received a subscription notification with an unknown tag %s", notificationTag));
		} else 
			try {
				sub.handleNotification(tuple);
			}catch(Throwable t) {
				Logging.log(t, String.format("error thrown from subscription notification handler for %s" , notificationTag));
			}
	}

	/*
	@Override
	public NotificationInducer<T> getInducer() {
		return subscription;}

	@SuppressWarnings("unchecked")
	@Override
	public void setInducer(NotificationInducer<?> inducer) {
		this.subscription = (Subscription<T>)inducer;}
		*/
}
