package calqlogic.twservercomms;

import java.io.IOException;
import java.time.Duration;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;


/**
 * A PolledQueryPeriodicSchedule is a schedule for polling at regular time intervals.
 * Although the period may be specified as a value (Duration) with nanosecond granularity,
 * the actual scheduling truncates this to milliseconds.
 * @author nmg
 *
 */
@JsonSerialize(using = PolledQueryPeriodicSchedule.ScheduleToSecSerializer.class)
public class PolledQueryPeriodicSchedule implements PolledQuerySchedule {
	private final Duration period;
	/**Create a new PolledQueryPeriodicSchedule
	 * @param period the interval at which polling will take place for polled queries using this schedule
	 */
	public PolledQueryPeriodicSchedule (Duration period) {
		this.period = period;}
	
	/**
	 * @return this schedule's polling interval
	 */
	public Duration getPeriod() {return period;}
	
	static class ScheduleToSecSerializer extends JsonSerializer<PolledQueryPeriodicSchedule> {
		@Override
	    public void serialize(PolledQueryPeriodicSchedule schedule, 
	                          JsonGenerator jsonGenerator, 
	                          SerializerProvider serializerProvider) 
	                          throws IOException {
	        jsonGenerator.writeObject(schedule.period.toMillis()/1000.);
	    }
	}
}
