package calqlogic.twservercomms;

import java.util.ArrayList;
import com.fasterxml.jackson.annotation.JsonProperty;
import nmg.softwareworks.jrpcagent.*;

//@JsonIgnoreProperties(ignoreUnknown = true,  value = { "handle"})
//@JsonDeserialize(using = QueryPushNotification.QPNDeserializer.class)
public class QueryPushNotification <T> extends Notification{
	private final boolean exhausted;
	//private final BatchRows<T> rows;
	private ArrayList<T> notifiedRows;
	
	//public QueryPushNotification(){} //for jackson deserialization
	public QueryPushNotification( @JsonProperty(value="tuples", required = false)BatchRows<T> rows,
	@JsonProperty(value="exhausted", required = false)Boolean exhausted, 
	// uninteresting properties sometimes present in the notification object
	@JsonProperty(value="handle", required = false)Integer handle,
	@JsonProperty(value="count", required = false)Integer count, @JsonProperty(value="total-count", required = false)Integer totalCount) {
		notifiedRows = (rows == null) ? new ArrayList<T>() : rows.getRows();;
		this.exhausted = (exhausted == null)? false : exhausted;
	}
	
	@Override 
	public void handle(Connection conn, String notificationMethod) {
		@SuppressWarnings("unchecked")
		var controller = (NotificationResultController<T>)this.getInducer();
		if (exhausted) { //mark resultset as closed, if there is one If all the results
			var rs = controller.getResultSet();
			if (rs != null) rs.close();
		}
		var more = controller.handleRows(notifiedRows, exhausted);
		if (more && !exhausted) {
			try {
				conn.synchronousRPC(Void.TYPE, "next-resultset-incremental", controller.controlParams());
			} catch (JRPCException e) {
				Logging.log("request to get additional rows by notification failed", e);
				var rs = controller.getResultSet();
				if (rs != null) rs.close();
			}
		}
	}

	/*@Override
	public NotificationInducer<T> getInducer() {return controller;}

	@SuppressWarnings("unchecked")
	@Override
	public void setInducer(NotificationInducer<?> inducer) {
		controller = (PushResultController<T>) inducer;	}
		*/
	
	/*private static class QPNDeserializer extends JsonDeserializer<QueryPushNotification<?>> {

		@Override
		public QueryPushNotification<?> deserialize(JsonParser jParser, DeserializationContext ctxt)
				throws IOException, JacksonException {
			var xxx = jParser.readValueAsTree();
			return null;
		}
	}*/
}
