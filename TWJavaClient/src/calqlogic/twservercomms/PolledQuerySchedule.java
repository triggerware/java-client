package calqlogic.twservercomms;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * PolledQuerySchedule is an interface for different classes of polled query schedules
 * A PolledQuerySchedule is used by the TW server to determine when to poll a ScheduledQuery. 
 * Every class that implements PolledQuerySchedule must implement asJson()  to produce a serialization acceptable to the Triggerware Server.
 */
public interface PolledQuerySchedule {
	JsonNode asJson();
}
