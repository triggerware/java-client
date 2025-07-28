package calqlogic.twservercomms;


import java.io.IOException;
import java.util.HashSet;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * a PolledQueryCombinedSchedule provides a schedule that will cause the TW server to poll a ScheduledQuery
 * at times that are the <em>union</em> of the times from individual calendar schedules.
 * This class is anticapated to be used with PolledQueryCalendarSchedules as the individual schedules.
 * Although the set of schedules contained in a combined schedule may be altered over time, changes have no impact
 * on a ScheduledQuery that has already been registered using the combined schedule.
 *
 */
@JsonSerialize(using = PolledQueryCombinedSchedule.Serializer.class)
public class PolledQueryCombinedSchedule {
	
	private final HashSet<PolledQueryCalendarSchedule> schedules = new HashSet<>();
	/**
	 * create a new PolledQueryCombinedSchedule with no component schedules
	 */
	public PolledQueryCombinedSchedule() {}

	/**
	 * Create a new PolledQueryCombinedSchedule and initialize it with one or more schedules.
	 * @param schedules the schedules populate this combined schedule.
	 */
	public PolledQueryCombinedSchedule(PolledQueryCalendarSchedule ... schedules) {
		for (var schedule:schedules) addSchedule(schedule);}

	/**
	 * determine if this this PolledQueryCombinedSchedule contains a specific schedule
	 * @param schedule the schedule to look for
	 * @return true if this PolledQueryCombinedSchedule contains the schedule
	 */
	public synchronized boolean contains (PolledQueryCalendarSchedule schedule){return schedules.contains(schedule);}
	
	/**
	 * add a schedule to this PolledQueryCombinedSchedule 
	 * @param schedule a PolledQuerySchedule to add to the combined schedule
	 * @return true if the schedule was added, false if the schedule was already part of the combined schedule.
	 */
	public synchronized boolean addSchedule(PolledQueryCalendarSchedule schedule) {
		return schedules.add(schedule);}
	
	/**
	 * @param schedule  a PolledQuerySchedule to remove from the combined schedule
	 * @return true if the schedule was removed, false if the schedule was not included in the combined schedule
	 */
	public boolean removeSchedule(PolledQueryCalendarSchedule schedule) {
		return schedules.remove(schedule);}
	
	//javadoc complains if the serializer class is private!
	/*private*/ static class Serializer extends JsonSerializer<PolledQueryCombinedSchedule> {

		@Override
		public void serialize(PolledQueryCombinedSchedule csched, JsonGenerator gen, SerializerProvider sp)
				throws IOException {
			gen.writeStartArray();
			for (var sched:csched.schedules) {
				gen.writeObject(sched);}
			gen.writeEndArray();			
		}		
	}
}
