package calqlogic.twservercomms;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import nmg.softwareworks.jrpcagent.Connection;
import nmg.softwareworks.jrpcagent.Logging;
import nmg.softwareworks.jrpcagent.Notification;

/**
 * A SubscriptionNotification represents a notification sent by the TW server due to some active subscription.
 * @param <T>  the type of row for any instance of this notification. 
 */
@JsonIgnoreProperties(ignoreUnknown = true) //just ignore label and update#
final class SubscriptionNotification <T> extends Notification{
	@JsonIgnore
	private Subscription<T> subscription;
	@JsonProperty("tuple")
	private T row;

	public SubscriptionNotification() {} //for jackson deserialization
	
	//this constructor is for use from the deserializer for Notifications
	@SuppressWarnings("unchecked")
	SubscriptionNotification(Subscription<?> subscription, Object row){
		this.row = (T)row;
		this.subscription = (Subscription<T>) subscription;		
	}
	/**
	 * @return the Subscription this notification responds to.
	 */
	public Subscription<T> getSubscription() {return subscription;}

	/**
	 * @return the r0w instance of this notification.
	 */
	public T getRow() {return row;}
	
	@Override
	public void handle(Connection conn, String notificationTag) {
		@SuppressWarnings("unchecked")
		var sub = (Subscription<T>)((TriggerwareClient)conn.getAgent()).getNotificationInducer(notificationTag);
		if (sub == null) {
			Logging.log(String.format("internal error: received a subscription notification with an unknown tag %s", notificationTag));
		} else 
			try {
				sub.handleNotification(row);
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
