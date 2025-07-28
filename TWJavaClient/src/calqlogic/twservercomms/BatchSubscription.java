package calqlogic.twservercomms;

import java.util.HashSet;

import calqlogic.twservercomms.Subscription.SubscriptionException;
import nmg.softwareworks.jrpcagent.JRPCException;
import nmg.softwareworks.jrpcagent.NotificationInducer;

/**
 * <p>In the TW server, changes to data can occur as part of transactions (see TW server documentation). The states of the data
 * prior to and following a transaction are the states referenced by the two-state condition of a Subscription.  It is possible
 * (in many applications it will be common) that the condition of a Subscription will be satisfied for multiple tuples of data
 * across a single transaction, and/or that the conditions of <em>multiple</em> Susbscriptions will be satisfied across a single
 * transaction.
 * </p><p>
 * When this occurs and the satisfied subscription conditions come from <em>independently</em> activated Subscription instances,
 * the multiple notifications arrive at the client sequentially in no particular order, and are handled by the client in arrival
 * order. That may be perfectly adequate in many applications. In others, the intermediate states implied by the sequential handling
 * of the notifications can cause problems.  In that case, use of a BatchSubscription may be useful.
 * </p><p>
 * A BatchSubscription groups one or more Subscription instances. Over time, new instances may be added to the BatchSubscription
 * and/or existing members may be removed.  An already active Subscription may not be added to a BatchSubscription.
 * A BatchSubscription may be activated on any one of a TriggerwareClient's connections.
 * When a BatchSubscription is active, <em>all</em> its members are active (being monitored by the TW server). 
 * When a Subscription is removed from an active BatchSubscription, that subscription is deactivated on the TW server.
 *</p><p>
 * When a transaction in the TW server triggers the condition of any of the Subscriptions in that BatchSubscription, a single
 * notification is sent to the client
 * </p><p>
 * A typical use of a BatchSubscription consists of sequentially
 * <ol>
 * <li> create a  new BatchSubscription instance with the constructor {@link #BatchSubscription() BatchSubscription()}.</li>
 * <li> add some Subscriptions to the BatchSubscription with{@link addSubscription addSubscription} (alternatively, use the constructor that
 * accepts an initial set of Subscriptions as a parameter)</li>
 * <li> activate the BatchSubscription with {@link #activate activate}</li>
 * </ol>
 * To handle the notifications, you can either
 * <ul>
 * <li>use you own subclass of BatchSubscription, overriding the {@link #handleNotification handleNotification} method.</li>
 * <li> override the (@link Subscription#handleNotificationFromBatch handleNotificationFromBatch} method of some/all of the 
 * subscriptions in the BatchSubscription</li>
 * </ul>
 * The handling of a notification from the BatchSubscription is carried out in the notification handling thread of the connection
 * on which the BatchSubscription was activated.  Any division of labor across threads, or attempt to free that thread to handle
 * later-arriving notifications, must be accomplished by code in method overrides.
 */
public class BatchSubscription implements NotificationInducer{

	private final String notificationTag = TriggerwareClient.nextNotificationMethod("bsub");
	private TriggerwareConnection subscribedOn = null;
	private final HashSet<Subscription<?>> subscriptions = new HashSet<Subscription<?>>();
	
	/**
	 * create a new, inactive, BatchSubscription instance containing no subscriptions
	 */
	BatchSubscription(){	}
	

	/**
	 * create a new, inactive, BatchSubscription instance and populate it with some Subscriptions
	 * @param subscriptions the subscriptions to include in the new BatchSubscription
	 */
	BatchSubscription(Subscription<?> ... subscriptions){	
		this();
		try {
			for (var sub : subscriptions) addSubscription(sub);
		} catch(JRPCException e) {} //the exception can only happen if the batchsubscription is active!
	}
	
	public String getDispatchString() {return notificationTag;}
	
	@Override
	public Object getNotificationType() {return BatchNotification.class;}
	
	/**
	 * @return the TriggerwareConnection on which this BatchSubscription's notifications arrive
	 * This will be null until the subscription has been activated.
	 */
	public synchronized TriggerwareConnection activeConnection() {return subscribedOn;}
	
	/**
	 * add an additional subscription to this BatchSubscription.  Activate the subscription if this BatchSubscription
	 * is currently active.
	 * @param subscription a subscription to add to this BatchSubscription
	 * @throws SubscriptionException if the subscription is already part of some other BatchSubscription, or if the
	 * subscription is already active as an individual subscription
	 * @throws JRPCException if the attempt to activate the subscription is rejected by  the TW server.
	 */
	public void addSubscription(Subscription<?> subscription) throws SubscriptionException, JRPCException {
		var batch = subscription.partOfBatch;
		if (batch!=null) {
			if (batch == this) return; //noop, already in this batch.
			throw new SubscriptionException("attempt to add a subscription to a second BatchSubscription", subscription);
		}
		if (subscription.activeConnection() != null)
			throw new SubscriptionException("attempt to add an already active subscription to a BatchSubscription", subscription);
		subscriptions.add(subscription);
		subscription.partOfBatch = this;
		if (subscribedOn != null) // activate immediately
			subscription.registerWithTW(subscribedOn);
	}
	
	public void activate(TriggerwareClient client) throws JRPCException {
		activate(client.getPrimaryConnection());}
	public synchronized void activate(TriggerwareConnection connection) throws JRPCException {
		if (subscribedOn != null) {
			if (subscribedOn == connection) return; //noop
			throw new SubscriptionException("attempt to activate a batch subscription on a second connection");
		}
		connection.getAgent().registerNotificationInducer(notificationTag, this);
		for (var subscription : subscriptions) 
			subscription.registerWithTW(subscribedOn);
	}

	/**
	 * <p>deactivate all the subscriptions in this BatchSubscription. This does not affect the set of Subscriptions
	 * conatained by the BatchSubscription; it just means that they will not be monitored by the TW server and thus
	 * no notifications will be sent.  The BatchSubscription may later be activated on the same or a different connection.
	 * deactivate is a noop if the BatchSubscription is not currently active.
	 * </p><p>
	 * Implementation note:  The TW server does not provide a means for deactivating multiple subscriptions 'atomically'.
	 * When you deactivate a BatchSubscription, its component subscriptions are deactivated sequentially.  This may create
	 * a race condition with transactions taking place in the server. Those transactions could result in notifications 
	 * being sent that contain only the notifications that arise from some still-active members of the BatchNotification. 
	 * </p>
	 * @throws JRPCException if the deactivation of any of the subscriptions fails for any reason
	 */
	public synchronized void deactivate() throws  JRPCException {
		if (subscribedOn == null) return;
		for (var subscription : subscriptions) subscription.unregisterWithTw();
		subscribedOn.getAgent().unregisterNotificationInducer(notificationTag);
		subscribedOn = null;
	}
	
	/**
	 * @param batch the BatchNotification containing a structured collection of individual notifications
	 * from the subscriptsions of this BatchSubscription
	 */
	public void handleNotification(BatchNotification batch) {
		for (var subBatch :batch.getNotifications().entrySet()) {
			var sub = subBatch.getKey();
			var tuples = subBatch.getValue(); //ArrayList of tuple deserializations
			sub.handleBatchNotifications(tuples);
		}		
	}

	@Override
	public boolean isClosed() {		return false;	}
}
