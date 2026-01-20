package calqlogic.twservercomms;

import java.time.Instant;
import com.fasterxml.jackson.annotation.*;
import nmg.softwareworks.jrpcagent.Connection;
import nmg.softwareworks.jrpcagent.Logging;
import nmg.softwareworks.jrpcagent.Notification;

@JsonIgnoreProperties(ignoreUnknown = true,  value = { "handle"})
class PolledQueryNotification <T> extends Notification{
	@JsonIgnore
	boolean initialized;
	@JsonProperty("timestamp")
	private String timeStamp; //TODO: make jackson deserialize string as Instant!
	//either delta or errorText will be null, the other non-null
	@JsonProperty("delta")
	private RowsDelta<T> delta;
	@JsonProperty("error")
	private String errorText;
	
	Instant timestampAsInstant() {
		return Instant.parse(timeStamp);}//temporary, until I get the string deserialized as an Instant

	public PolledQueryNotification(){} //for jackson deserialization

	/**
	 * @return the row instance of this notification.
	 */
	public RowsDelta<T> getdelta() {return delta;}
	
	@Override
	public void handle(Connection conn, String notificationTag) {
		@SuppressWarnings("unchecked")
		var pq = ((PolledQuery.PolledQueryNotificationInducer<T>)this.getInducer()).getQuery();//((TriggerwareConnection)conn).getPolledQueryFromHandle(handle);
		Instant ts = timestampAsInstant();
		try {
			if (errorText!=null) pq.handleError(errorText, ts);
			else {
				//Logging.log("handling polled query notification");
				pq.handleSuccess(delta, ts);
			    pq.hasSucceeded = true;
			 }
		}catch(Throwable t) {
			Logging.log(t, String.format("error thrown from polled query notification handler for %s" , notificationTag));
		} 
	}
}
