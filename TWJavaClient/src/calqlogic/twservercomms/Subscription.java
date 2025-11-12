package calqlogic.twservercomms;

import java.lang.reflect.Constructor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import calqlogic.twservercomms.AbstractQuery.SignatureElement;
import nmg.softwareworks.jrpcagent.*;

/**
 * <p>A Subscription represents, via a two-state condition (see TW server documenation), some change to the data managed by the
 *  TW server about which a client would like to be notified.  The two-state condition determines the form of notification as 
 *  a row of values, just like a row in a resultset returned from a one-state query. The subscription's generic parameter
 *  specifies the java class into which the row of values in a notification should be deserialized and made available to the
 *  subscribing client.
 *  The subscription's   {@link #handleNotification} method provides the means
 *  for the client to handle notifications of the triggering of the condition detected by the TW server.
 * </p><p>,<em>Creating</em> a subscription does not necessitate any interaction with the TW server, and does not detect any
 * errors that might exist in the text that defines the tw-state condition. The {@link #activate} method activates that
 * subscription on the server so that notifications of changes that meet the condition are sent to the client.  
 * </p><p>A subscription may be activated on any of the client's connections at any time, but may be active on only 
 * one connection at any time.  Notifications resulting
 * from an active Subscription arrive on the connection on which that Subscription was activated.  Activating a subscription
 * will only cause notifications to be sent about changes that occur <em>after</em> the activation is accepted by the TW server,
 * so there <em>could</em> be a race between sending an activate request and changes to data that are not synchronized with respect to
 * that activation.
 * </p><p> When a notification from  an active subscription arrives at the TriggerwarClient, it is dispatched to the subscription's
 * {link #handleNotification} method. Execution of the handleNotification methods for subscriptions -- whether the same 
 * or different subscriptions -- are serialized with respect to one another and with respect to notifications 
 * for active PolledQueries.  This means that, on any
 * given connection, the notifications are handled sequentially, in arrival order.  A <em>handleNotification</em> method is of
 * course free to use another thread to perform time consuming actions, freeing the connection's notification handling thread
 * to handle later arriving notifications.
 * </p><p>The {@link #deactivate} method deactivates the Subscription. Deactivating a subscription
 * has no effect on notifications previously sent by the server but not yet processed by the client. As with activation, there
 * can be a race between sending the deactivate request and changes to data that are not synchronized with respect to making that
 * request.  The client is assured that no additional notifications will be transmitted by the server <em>after</em>the server has
 * processed the deactivate request.
 * </p><p>The TW server is incapable of detecting, and thus notifying about, changes satisfying some  two-state conditions.
 * The only way of determining whether notifications of changes  are supported for a particular condition is to activate
 * a subscription using that condition. The activate request throws an exception  for unsupported conditions.
 * </p>

 * <h2> Individual vs. Batch Notification </h2>
 * <p>In the TW server, changes to data can occur as part of transactions (see TW server documentation). The states of the data
 * prior to and following a transaction are the states referenced by the two-state condition of a Subscription.  It is possible
 * (in many applications it will be common) that the condition of a Subscription will be satisfied for multiple rows of data
 * across a single transaction, and/or that the conditions of <em>multiple</em> Subscriptions will be satisfied across a single
 * transaction.  Flexibility in dealing with this possibility is provided by distinguishing between individual subscriptions and
 * batch subscriptions.
 * </p><p> Subscriptions activated (via {@link #activate activate}) for individual notification will result in a 
 * sequence of notifications being sent
 * to the client in these cases, and those notifications will be handled as just described above.  
 * The order in which these arrive is arbitrary, and multiple notifications for a single subscription
 * may be interleaved with those of another.  For many notifications, the fact that multiple interesting changes occurred in a 
 * single transaction is not significant, and handling them sequentially is acceptable.
 * </p><p> 
 * An alternative is to group one or more Subscriptions in a {@link BatchSubscription}. When activated for batch notification,
 * all the notifications for all the Subscriptions in that BatchSubscription resulting from a single transaction
 *  are delivered in a single notification and can be processed by a single handler.
 * @param <T> <p>The Java class that will be used to represent a single row of values that triggers the subscription's condition.
 * Each row arriving in a notification from the subscription will <em>deserialize</em> into a freshly allocated instance of this class.
 * This class cannot be a primitive java type.  Often it will be a Java 'bean' with suitable annotations for deserialization.
 * A row is <em>serialized</em> by the TW server as a json array whose elements are determined by the subscription's triggering condition.
 * </p><p>
 * The class may be Object[], or some generic Collection class with Object as the type parameter (e.g., ArrayList&lt;Object&gt;), in
 * which case a row will be deserialized as an Array or Collection whose elements are the default deserialization for whatever
 * Json text appears in the row's serial representation.
 *</p><p>
 In most uses of Subscriptions, the triggering condition is known at code-authoring time, 
 and thus so is the 'type signature' of the triggering condition, which determines the serialization. It is almost always
 possible to provide a more specific row type for a Subscription than simply Object[] -- usually one that is far more useful
 in coding the handling of the notification than simply an array or collection of Objects.
 * </p>
 * @see BatchSubscription
 */
public abstract class Subscription<T> implements NotificationInducer{
	private static class SSignature{
		private final SignatureElement[]signature;
		@JsonCreator
		SSignature( @JsonProperty("signature")SignatureElement[] signature){
			this.signature = signature;
		}
	}
	private static NamedParameterRequest<SSignature> subscribeRequest = 
			new NamedParameterRequest<SSignature>(SSignature.class, "subscribe", 
					new String[] {"description", "language","package", "label", "method"}, new String[] {"combine"});
	private static NamedParameterRequest<Void> unsubscribeRequest = 
			new NamedParameterRequest<Void>(Void.TYPE, "unsubscribe", 
					new String[] {"description", "label", "method", "combine"}, null);
	
	private static final int subscriptionErrorCode = -32701;
	/**
	 * A SubscriptionException is thrown due to problems encountered when activating a subscription.
	 */
	public static class SubscriptionException extends JRPCException{
		private final Subscription<?> subscription;
		SubscriptionException (String problem, Subscription<?> s) {
			super(problem, subscriptionErrorCode);
			this.subscription = s;
		}
		public SubscriptionException(String problem) {
			super(problem, subscriptionErrorCode);
			subscription = null;
		}
		/**
		 * @return the subscription that encountered a problem.
		 */
		public Subscription<?>getSubscription(){return this.subscription;}
	}

	private TriggerwareConnection subscribedOn = null;
	protected final String triggeringCondition;
	protected final String notificationTag = TriggerwareClient.nextNotificationMethod("sub");
	//@SuppressWarnings("unused")
	//private final String language;
	//private String  batchTag = null;
	BatchSubscription partOfBatch = null;
	private final NamedRequestParameters namedParameters;
	protected final String schema;
	//two of these will be null
	protected final Class<T> rowClass;
	//protected final TypeReference<T> rowTypeRef;
	protected final JavaType rowJType;
	private Object deserializationType; //set when the client owning the subscription is known

	//private final Object rowSignature;
	protected  SignatureElement[]signature;
	protected final Constructor<T> rowConstructor;
	
	/**
	 * @param rowType  the class of the notifications from this subscription
	 * @param triggeringCondition  the two-state SQL condition for the subscription
	 * @param schema the default schema for the triggeringCondition
	 */
	public Subscription(Class<T> rowType, String triggeringCondition, String schema) {
		this((Object)rowType, triggeringCondition, schema/*, Language.SQL*/);	}

	/**
	 * @param rowType  the class of the notifications from this subscription
	 * @param triggeringCondition  the two-state SQL condition for the subscription
	 * @param schema the default schema for the triggeringCondition
	 */
	/*public Subscription(TypeReference<T> rowType, String triggeringCondition, String schema) {
		this((Object)rowType,  triggeringCondition, schema, Language.SQL);}*/

	/**
	 * This constructor is for reflective applications, where the row type is not known at compile time.
	 * @param rowType  the class of the notifications from this subscription
	 * @param triggeringCondition  the two-state SQL condition for the subscription
	 * @param schema the default schema for the triggeringCondition
	 */
	public Subscription(JavaType rowType, String triggeringCondition, String schema) {
		this( (Object)rowType,  triggeringCondition, schema /*, Language.SQL*/);	}

	/**
	 * @param rowType  the class of the notifications from this subscription
	 * @param triggeringCondition  the two-state condition for the subscription
	 * @param schema the default schema for the triggeringCondition
	 * @param language the appropriate members of  {@link Language} for the triggeringCondition
	 */
	/*public Subscription(Class<T> rowType, String triggeringCondition, String schema , String language) {
		this((Object)rowType, triggeringCondition, schema, language);	}*/

	/**
	 * @param rowType  the class of the notifications from this subscription
	 * @param triggeringCondition  the two-state condition for the subscription
	 * @param schema the default schema for the triggeringCondition
	 * @param language the appropriate members of  {@link Language} for the triggeringCondition
	 */
	/*public Subscription(TypeReference<T> rowType, String triggeringCondition, String schema, String language) {
		this((Object)rowType,  triggeringCondition, schema, language);}*/

	/**
	 * This constructor is for reflective applications, where the row type is not known at compile time.
	 * @param rowType  the class of the notifications from this subscription
	 * @param triggeringCondition  the two-state condition for the subscription
	 * @param schema the default schema for the triggeringCondition
	 * @param language the appropriate members of  {@link Language} for the triggeringCondition
	 */
	/*public Subscription(JavaType rowType, String triggeringCondition, String schema, String language) {
		this( (Object)rowType,  triggeringCondition, schema, language);	}*/
	protected NamedRequestParameters commonParameters() {
		return new NamedRequestParameters()
				  .with("query", triggeringCondition)
				  .with("language", Language.SQL)
				  .with("label", notificationTag);
	}
	
	public NamedRequestParameters getNamedParameters() {return namedParameters;}

	/**
	 * @param rowType  the type for the notifications from this subscription
	 * @param triggeringCondition  the two-state condition for the subscription
	 * @param schema the default schema for the triggeringCondition
	 * @param language the appropriate members of  {@link Language} for the triggeringCondition
	 */
	@SuppressWarnings("unchecked")
	Subscription(Object rowType, String triggeringCondition, String schema/*, String language*/) {
		this.triggeringCondition = triggeringCondition;
		this.schema = schema;
		//this.language = language;
		if (rowType instanceof Class<?>) {
			rowClass = (Class<T>) rowType;
			rowConstructor = AbstractQuery.getRowConstructor(rowClass);
			//rowTypeRef = null;
			rowJType = null;
		} /*else if (rowType instanceof TypeReference<?>){//probably will remove this option.
			rowClass = null;
			rowTypeRef = (TypeReference<T>) rowType;
			rowJType = null;
		}*/ else {//uses a JavaType
			rowClass = null;
			//rowTypeRef = null;
			rowJType = (JavaType) rowType;
			rowConstructor = null;
		} 
		//this.rowSignature = rowType;
		namedParameters = commonParameters();
		if (schema != null)namedParameters.with("namespace", schema);
	}

	private void setDeserializationType(TriggerwareClient client) {
		var tf = TypeFactory.defaultInstance();
		if (rowClass != null) {
			deserializationType = tf.constructParametricType(SubscriptionNotification.class, rowClass);
		} /*else if (rowTypeRef != null){
			deserializationType = rowTypeRef ;
		} */else {//uses a JavaType
			deserializationType = tf.constructParametricType(SubscriptionNotification.class, rowJType);
		} 
		
	}
	@Override
	public Object getNotificationType() {return deserializationType;}
	
	/*T deserializerow(ArrayNode jrow, Connection connection) { 
		var twom = connection.getStreamMapper();
		if (rowClass != null)
		  return twom.convertValue(jrow, rowClass);
		if (rowTypeRef != null)
			return  twom.convertValue(jrow, rowTypeRef);
		return null;
	}*/
	
	private String getDispatchString() {return notificationTag;}
	
	//Object getrowSignature(){return rowSignature;}
	/**
	 * @return the triggering condition of this subscription
	 */
	public String getTriggeringCondition() {return triggeringCondition;}
	/**
	 * @return the default schema for the triggering condition of this subscription.
	 */
	public String getSchema() {return schema;}
	
	/*private Object computeDeserializationType(TriggerwareConnection connection) {
		var tf = connection.getTypeFactory();
		if (rowJType != null)
			return tf.constructParametricType(PolledQueryNotification.class, rowJType);
		else if (rowClass != null) 
			return tf.constructParametricType(PolledQueryNotification.class, rowClass);
		else return rowTypeRef; //hope to eliminate this
	}*/
	
	/**
	 * @return the connection on which the subscription is currently activated, or null if the subscription is not activated
	 * If this subscription is part of a batch subscription, this is equivalent to retrieving the active connction of that batch
	 * subscription.
	 */
	public synchronized TriggerwareConnection activeConnection() {
		if (partOfBatch != null) return partOfBatch.activeConnection();
		return subscribedOn;}

	/**
	 * Activate a subscription for individual notifications 
	 * @param client  the client which should monitor this subscription.
	 * Notifications will arrive on the client's primary connection
	 * @throws SubscriptionException if the client
	 * <ul>
	 * <li> already has this subscription active on a different connection</li>
	 * <li> this subscription is part of a BatchSubscription </li>
	 * </ul>
	 * @throws JRPCException  if the activation fails for any other reason
	 */
	public void activate(TriggerwareClient client) throws SubscriptionException, JRPCException {
		activate(client.getPrimaryConnection());}
	/**
	 * Activate a subscription for individual notifications.  The connection's TriggerwareClient
	 * will monitor the subscription.
	 * @param connection the connection on which notifications will arrive.
	 * @throws SubscriptionException if 
	 * <ul>
	 * <li> this subscription is currently active on a different connection</li>
	 * <li> this subscription is part of a BatchSubscription </li>
	 * </ul>
	 * @throws JRPCException  if the activation fails for any other reason
	 */
	public synchronized void activate(TriggerwareConnection connection) throws SubscriptionException, JRPCException {
		if (partOfBatch != null)
			throw new SubscriptionException("attempt to activate a subscription as an individual subscription when it is part of a batch", this);
		if (subscribedOn == connection) return; //already active on the requested connection
		if (subscribedOn != null)
			throw new SubscriptionException("subscription is already active on a different connection", this);
		var client = connection.getClient();
		setDeserializationType(client);
		client.registerNotificationInducer(notificationTag, this);
		try {
			registerWithTW(connection);
			subscribedOn = connection;
		}catch(JRPCException e) {
			client.unregisterNotificationInducer(notificationTag);
			deserializationType = null;
			throw e;
		}
	}
	
	/**
	 * Deactivate this subscription so that the TW server ceases to monitor for changes in its condition and 
	 * sends no further notifications of such changes.  This is a noop if this subscription is not currently active.
	 * @throws SubscriptionException if this subscription is part of a BatchSubscription
	 * @throws JRPCException if the deactivation fails for any reason
	 */
	public void deactivate() throws SubscriptionException, JRPCException {
		if (partOfBatch != null)
			throw new SubscriptionException("attempt to deactivate a subscription as an individual subscription when it is part of a batch", this);
		unregisterWithTw();
		subscribedOn.getAgent().unregisterNotificationInducer(notificationTag);		
	}

	void unregisterWithTw() throws JRPCException {
		unsubscribeRequest.execute(subscribedOn, namedParameters);
		subscribedOn = null;
	}
	
	protected void registerWithTW(TriggerwareConnection connection) throws JRPCException {
		namedParameters.with("combine", partOfBatch != null)
					   .with("method",  (partOfBatch==null) ? getDispatchString() : partOfBatch.getDispatchString());
		var ssignature = subscribeRequest.execute (connection, namedParameters);
		signature = ssignature.signature;
	}

	/**
	 * handleNotification is called to respond to a triggering of the subscription's condition. Any instantiable subclass 
	 * of Subscription must implement this method.  The method may not throw any <em>checked</em> exceptions. If a handleNotification
	 * method throws an <em>unchecked</em> exception, the exception will be logged and ignored by the TriggerwareClient.
	 * @param row the object allocated to hold the 'row' of values that represents a triggering of the 
	 * subscription's condition.	 * 
	 */
	public abstract void handleNotification(T row);
	
	/**
	 * handleNotificationFromBatch is called to respond to a triggering of the subscription's condition when the subscription
	 * is part of a BatchSubscription and the <em>default</em> handling of the batch subscription is being employed.
	 * The default implementation of handleNotificationFromBatch simply calls the subscription's handleNotification method.
	 * @param row the object allocated to hold the 'row' of values that represents a triggering of the 
	 * subscription's condition.
	 */
	public void handleNotificationFromBatch(T row) {handleNotification(row);}

	@SuppressWarnings("unchecked")
	void handleBatchNotifications(java.util.Collection<?> rows) {
		for (var row : rows) {
			handleNotificationFromBatch((T)row);	}
	}

	@Override
	public void establishDeserializationAttributes(SerializationState ss) {
		//enable this code once the subscribe request returns a signature
		/*ss.put("rowSignature", signature);
		if (rowConstructor != null)
			ss.put("rowBeanConstructor", rowConstructor);*/
	};
}
