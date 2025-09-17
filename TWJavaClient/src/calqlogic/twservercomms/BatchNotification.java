package calqlogic.twservercomms;

import java.io.IOException;
import java.util.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
//import com.fasterxml.jackson.databind.module.SimpleModule;
import nmg.softwareworks.jrpcagent.*;
//import nmg.softwareworks.jrpcagent.JsonUtilities.JRPCObjectMapper;

/**
 * A BatchNotification comprises multiple notifications received in response to a single transition on the TW server.
 * BatchNotification groups notifications from one or more Subscriptions belonging to a single {@link BatchSubscription}.
 * A BatchNotification is supplied as the parameter of a BatchSubscription's 
 * {@link BatchSubscription#handleNotification handleNotification} method.
 * These notifications are represented as a Map, using the Subscription as the key and a collection of tuples as the value
 * for each Map entry.
 *
 */
//@JsonDeserialize(using = BatchNotification.BatchNotificationDeserializer.class)
public class BatchNotification  extends Notification{
	//private final String batchTag;
	//private final Connection connection;
	//private BatchSubscription bs = null;
	private final HashMap<Subscription<?>, Collection<?>> notifications =	new HashMap<>();
	BatchNotification(){};
	/*BatchNotification(String batchTag, ArrayNode matches, TriggerwareConnection connection){
		//this.connection = connection;
		//this.batchTag = batchTag;

	
		
		notifications = new Hashtable<Subscription<?>, ArrayList<SubscriptionNotification<?>>> (matches.size());
		var twClient = (TriggerwareClient)connection.getAgent();
		for (JsonNode notificationGroup : matches) {
			var nextGroup = (ObjectNode)notificationGroup;
			var subscriptionTag = nextGroup.get("label").asText();
			var ni = twClient.getNotificationInducer(subscriptionTag);
			if (ni == null || !(ni instanceof Subscription<?>)) {
				Logging.log("Batch subscription notification contains unknown subscription tag %s", subscriptionTag);
				continue;
			}
			var subscription = (Subscription<?>)ni;
			var tuples = (ArrayNode)nextGroup.get("tuples");
			var groupNotifications = new ArrayList<SubscriptionNotification<?>>();
			for (var jtuple : tuples) {
				var notification = new SubscriptionNotification(subscription, (ArrayNode)jtuple, connection);
				groupNotifications.add(notification);
			}
			if (!groupNotifications.isEmpty())
				notifications.put(subscription, groupNotifications);
		}
	}*/

	/**
	 * @return The notifications in this BatchNotification.  These notifications are factored by Subscription.
	 */
	public Map<Subscription<?>, Collection<?>> getNotifications(){return notifications;}
	@Override
	public void handle(Connection conn, String notificationTag) {
		var bsub = (BatchSubscription)((TriggerwareClient)conn.getAgent()).getNotificationInducer(notificationTag);
		if (bsub == null) {
			Logging.log(String.format("internal error: received a batch subscription notification with an unknown tag %s", notificationTag));
		} else 
			try {
				bsub.handleNotification(this);
			}catch(Throwable t) {
				Logging.log(t, String.format("error thrown from subscription notification handler for %s" , notificationTag));
			}
	}
	
	/* {..."method":"bsub3","params":
       {"update#" : 17,
        "matches": [{"label": "sub1", "tuples": [[3,"foo"],[4,"bar"]]},
                    {"label": "sub2", "tuples": [[3,4,"fum"],[4,5,"baz"]]}]} ...} 
             */
	
	/*static class DeserializationModule extends SimpleModule{
		//private final JRPCAgent agent;
		DeserializationModule(TriggerwareConnection twConnection){
			//this.agent = agent;
			addDeserializer(BatchNotification.class, new BatchNotificationDeserializer(twConnection) );
		}
	}*/
	static class  BatchNotificationDeserializer extends StdDeserializer<BatchNotification> {
		private final TriggerwareConnection twConnection;
		BatchNotificationDeserializer(TriggerwareConnection connection) {
	    	super(BatchNotification.class);  
	    	this.twConnection = connection;
	    }

		@Override
		public BatchNotification deserialize(JsonParser jParser, DeserializationContext ctxt)
				throws IOException, JsonProcessingException {
			//should deserialize the params property value of a batch subscription notification
			var mapper = (ObjectMapper)jParser.getCodec();//twConnection.getPartnerMapper();
			var token = jParser.nextToken();
			if (token != JsonToken.START_OBJECT) {
				var ignore = jParser.readValueAsTree();
				Logging.log("serialization error for a BatchNotification -- non object %s", ignore);
				return null;
			}
			var bn = new BatchNotification();
			while (true) {//parse individual fields
				var fieldname = jParser.nextFieldName();
				if (fieldname==null) { 
					token = jParser.getCurrentToken();
					if (token == JsonToken.END_OBJECT) break;
					Logging.log("unexpected end of BatchNotification on input stream");
					return null;
				}
				switch(fieldname) {
					case "update#" -> jParser.readValueAsTree();
					case "matches" -> parseMatches(jParser, mapper, ctxt,  bn.notifications);
					default -> jParser.readValueAsTree();
				}
			}
			return bn;
		}
		
		private void parseMatches(JsonParser jParser, ObjectMapper mapper, DeserializationContext ctxt,
				Map<Subscription<?>, Collection<?>> notifications ) throws IOException {

			//var tf = ctxt.getTypeFactory();
			//var twClient = (TriggerwareClient)mapper.getAgent();
			var token = jParser.nextToken();
			if (token != JsonToken.START_OBJECT) {
				var ignore = jParser.readValueAsTree();
				throw new IOException(String.format("serialization error for a BatchNotification -- matches is not an object %s", ignore));
			}
			var twClient = twConnection.getAgent();
			while (true) {//parse notifications from individual subscriptions
				var fieldname = jParser.nextFieldName();
				if (fieldname == null) { 
					token = jParser.getCurrentToken();
					if (token == JsonToken.END_OBJECT) break;
					throw new IOException("unexpected end of BatchNotification matches on input stream");
				}
				var inducer = (Subscription<?>)twClient.getNotificationInducer(fieldname);
				//if (! (inducer instanceof Subscription<?>))	inducer = null;
				var tupleType = inducer==null? null :inducer.getNotificationType(); //the type for a tuple
				if (tupleType == null) { // unregistered
					 jParser.readValueAsTree(); // consume the tuples
					throw new JRPCRuntimeException.UnknownMethodFailure(fieldname);
				}
				var subscription = (Subscription<?>)inducer;
				MappingIterator<?> mi = null;
				//var notificationsForSubscription = new ArrayList<SubscriptionNotification<?>>();
				//JavaType tuplesType = null;
				if (subscription.rowClass != null)
					mi = mapper.readValues(jParser, subscription.rowClass);
					//tuplesType = tf.constructParametricType(ArrayList.class, (Class<?>) tupleType);
				/*else if (subscription.tupleTypeRef != null)
					mi = mapper.readValues(jParser, subscription.tupleTypeRef);
					//tuplesType = tf.constructParametricType(ArrayList.class, tf.constructType((TypeReference<?>) tupleType));
				*/ else 
					mi = mapper.readValues(jParser, subscription.rowJType);
					//tuplesType = tf.constructParametricType(ArrayList.class, (JavaType) tupleType);
				var tuples = new ArrayList<Object>();
				while (mi.hasNext()) {
					tuples.add(mi.next());}
				mi.close();
				notifications.put(subscription, tuples);
			}
		}
	}
	/*@Override
	public NotificationInducer<Object> getInducer() {return bs;}
	@Override
	public void setInducer(NotificationInducer<?> inducer) {
		bs = (BatchSubscription)inducer;}*/
}
