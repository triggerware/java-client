package calqlogic.twservercomms;

import java.time.Instant;
import com.fasterxml.jackson.annotation.*;
import nmg.softwareworks.jrpcagent.Connection;
import nmg.softwareworks.jrpcagent.Logging;
import nmg.softwareworks.jrpcagent.Notification;

@JsonIgnoreProperties(ignoreUnknown = true,  value = { "handle"})
class PolledQueryNotification <T> extends Notification{
	//@JsonProperty("handle")
	//@JsonDeserialize(using = HandleToQueryDeserializer.class)
	//@JsonIgnore
	//private PolledQuery<T> pq;
	@JsonProperty("timestamp")
	private Instant timeStamp;
	//either delta or errorText will be null, the other non-null
	@JsonProperty("delta")
	private RowsDelta<T> delta;
	@JsonProperty("error")
	private String errorText;
	//@JsonProperty("handle")
	//private int serverHandle;

	public PolledQueryNotification(){} //for jackson deserialization

	/**
	 * @return the row instance of this notification.
	 */
	public RowsDelta<T> getdelta() {return delta;}
	
	//public PolledQuery<T> getPolledQuery(){return pq;}
	
	@Override
	public void handle(Connection conn, String notificationTag) {
		@SuppressWarnings("unchecked")
		var pq = (PolledQuery<T>)this.getInducer();//((TriggerwareConnection)conn).getPolledQueryFromHandle(handle);
		/*if (pq == null) {
			Logging.log(String.format("internal error: received a polledQuery notification with an unknown handle %d", handle));
		} else */
		try {
			if (errorText!=null) pq.handleError(errorText, timeStamp);
			else {	pq.handleSuccess(delta, timeStamp);
				    pq.hasSucceeded = true;
				 }
		}catch(Throwable t) {
			Logging.log(t, String.format("error thrown from polled query notification handler for %s" , notificationTag));
		} 
	}

	/*@Override
	public NotificationInducer<T> getInducer() {
		return pq;}

	@SuppressWarnings("unchecked")
	@Override
	public void setInducer(NotificationInducer<?> inducer) {
		pq = (PolledQuery<T>)inducer;}*/
	
	/*private static class HandleToQueryDeserializer extends StdDeserializer<Class> {

	    public HandleToQueryDeserializer() {super(PolledQuery.class);}

	    @Override
	    public Class<?> deserialize(JsonParser parser, DeserializationContext deserializer) throws IOException {
	    	int handle = parser.getValueAsInt();
	    	//need a connection object to get to a client to get to a map handle->pq
	        return null;
	    }
	}*/
}
