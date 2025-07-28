package calqlogic.twservercomms;

import java.util.ArrayList;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import nmg.softwareworks.jrpcagent.*;

@JsonIgnoreProperties(ignoreUnknown = true,  value = { "handle"})
public class QueryPushNotification <T> extends Notification{
	//@JsonProperty("resultset-handle")
	//private int handle;
	@JsonProperty("exhausted")
	private boolean exhausted;
	@JsonProperty("tuples")
	private ArrayList<T> rows;
	
	public QueryPushNotification(){} //for jackson deserialization
	
	@Override 
	public void handle(Connection conn, String notificationMethod) {
		@SuppressWarnings("unchecked")
		//var controller = (PushResultController<T>)((TriggerwareConnection)conn).getQueryResultControllerFromHandle(handle);
		var controller = (PushResultController<T>)this.getInducer();
		/*if (controller == null) {
			Logging.log(String.format("internal error: received a queryResult notification with an unknown handle %d", handle));
		} else */
			 controller.handleResults(rows, exhausted);
	}

	/*@Override
	public NotificationInducer<T> getInducer() {return controller;}

	@SuppressWarnings("unchecked")
	@Override
	public void setInducer(NotificationInducer<?> inducer) {
		controller = (PushResultController<T>) inducer;	}
		*/
}
