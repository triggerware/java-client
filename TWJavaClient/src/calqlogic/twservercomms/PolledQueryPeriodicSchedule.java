package calqlogic.twservercomms;

import java.time.Duration;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.DoubleNode;
import calqlogic.twservercomms.TriggerwareClient.TriggerwareClientException;


/**
 * A PolledQueryPeriodicSchedule is a schedule for polling at regular time intervals.
 * The period must be specified as a value (java.time.Duration) with nanosecond granularity
 * The TW server implementation determines the actual precision of periodic scheduling.
 */
//@JsonSerialize(using = PolledQueryPeriodicSchedule.ScheduleToSecSerializer.class)
public class PolledQueryPeriodicSchedule implements PolledQuerySchedule {
	protected final Duration period;
	/**Create a new PolledQueryPeriodicSchedule
	 * @param period the interval at which polling will take place for polled queries using this schedule
	 * @throws TriggerwareClientException 
	 */
	public PolledQueryPeriodicSchedule (Duration period) throws TriggerwareClientException {
		this.period = period;
		if (period.isZero() || period.isNegative()) //in java 18+, duration has an isPositive() method.
			throw new TriggerwareClientException("PolledQueryPeriodicSchedule must have a positive value for its period.");
	}
	
	/**
	 * @return this schedule's polling interval
	 */
	public Duration getPeriod() {return period;}
	
	
	/*static class ScheduleToSecSerializer extends JsonSerializer<PolledQueryPeriodicSchedule> {
		@Override
	    public void serialize(PolledQueryPeriodicSchedule schedule,  JsonGenerator jsonGenerator, SerializerProvider serializerProvider) 
	                          throws IOException, JsonProcessingException {
	        jsonGenerator.writeObject(schedule.period.toMillis()/1000.0); }
	}*/

	@Override
	public JsonNode asJson() {
		return new DoubleNode(period.toMillis()/1000.0);}
}
