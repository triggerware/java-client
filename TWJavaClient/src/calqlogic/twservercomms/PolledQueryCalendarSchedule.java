package calqlogic.twservercomms;

import java.util.TimeZone;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

/**
 * A PolledQueryCalendarSchedule defines a calendar based schedule used to run a polled query.
 * It contains 'filtering' fields for minutes, hours, days, months, and weekdays.
 * It contains a timezone, relative to which these filtering field's values are interpreteted.
 * 
 * A timestamp is considered to be in the schedule if all 5 pieces of the timestamp satisfy the
 * corresponding filtering field of the schedule.
 * 
 * The schedule is passed to the tw server when a ScheduledQuery is registered with the TW server.
 * The query is polled at the times specified by the filters in the specified timezone.  The timezone is specified as a string.
 * The strings allowed are the timezone names in IANA timezone database.  The string "UTC" is also allowed if you want to use UTC
 * rather than an actual timezone for a calendar schedule.
 * 
 * The values of filtering fields are strings having one of the following formats:
 * <ul>
 * <li> A single asterisk means that all timestamp field values satisfy the filter</li>
 * <li> A single decimal integer within the valid range of the field (e.g, between 1 and 31, inclusive, for days). 
 *  Only timestamps whose corresponding field has the specified value satisfy the filter.  </li>
 * <li> A pair of decimal integers separated by a single hyphen  (Min - Max).  
 * Both integers Min and Max must fall within the valid range of the field.
 * Max must be greater than or equal to Min.  This field value specifies the range of values from Min through Max inclusive.
 * Only timestamps whose corresponding field falls within that range satisfy the filter.  </li>
 * <li> A comma-separated list of the above  (e.g., "2, 4, 8-15")
 * </ul>
 * The ranges of values for the filtering fields are:
 * <ul>
 * <li>minutes 0-59</li>
 * <li>hours 0-23</li>
 * <li>days 1-31</li>
 * <li>months 1-12</li>
 * <li>weekdays 0-6  0 is Sunday, 6 is Saturday</li>
 * </ul>
  *
 */
public class PolledQueryCalendarSchedule implements PolledQuerySchedule {
	
	/**
	 * A ScheduleFormatException is thrown if an attempt is made to set a filtering field to an illegal value.
	 *
	 */
	public static class ScheduleFormatException extends Exception{
		ScheduleFormatException(String message, Throwable t) {super(message,t);}
	}

	private  static final String localTimezoneName =  TimeZone.getDefault().getID();

	/**
	 * create a new PolledQuerySchedule with default filtering fields.
	 * These defaults are: minutes="0", hours="0", days="*", months="*", weekdays="*"
	 * The timezone is initialized to 
	 * java.util.TimeZone.getDefault()
	 * This is the timezone of the JVM on which the TriggerwareClient is running.  This can be
	 * set explicitly, but is normally allowed to be initialized from settings in the operating system.
	 * The filtering field values and timezone can be changed at any time, but changes
	 * made <em>after</em> a PolledQueryCalendarSchedule using the schedule has been registered
	 * have not impact on the scheduling of that query.
	 */
	public PolledQueryCalendarSchedule() {}
	private String minutes="0", hours="0", days="*", months="*", weekdays="*";
	private String timezoneName = localTimezoneName;
	/**
	 * @return the minutes filtering field value
	 */
	public String getMinutes() {return minutes;}
	/**
	 * @return the hours filtering field value
	 */
	public String getHours() {return hours;}
	/**
	 * @return the days filtering field value
	 */
	public String getDays() {return days;}
	/**
	 * @return the months filtering field value
	 */
	public String getMonths() {return months;}
	/**
	 * @return the weekdays filtering field value
	 */
	public String getWeekdays() {return weekdays;}
	/**
	 * @return the timezone name of this schedule
	 */
	public String getTimezoneName() {return timezoneName;}
	
	/**
	 * set the minutes filter field of this PolledQueryCalendarSchedule
	 * @param minutes the new filter field value for the minutes field
	 * @return this PolledQueryCalendarSchedule
	 * @throws ScheduleFormatException if the minutes value is illegal
	 */
	public PolledQueryCalendarSchedule setMinutes(String minutes) throws ScheduleFormatException {
		validate(minutes, "minutes", 0, 59);
		this.minutes = minutes;
		return this;
	}

	/**
	 * set the hours filter field of this PolledQueryCalendarSchedule
	 * @param hours the new filter field value for the hours field
	 * @return this PolledQueryCalendarSchedule
	 * @throws ScheduleFormatException if the minutes value is illegal
	 */
	public PolledQueryCalendarSchedule setHours(String hours) throws ScheduleFormatException{
		validate(hours, "hours", 0, 23);
		this.hours = hours;
		return this;
	}

	/**
	 * set the days filter field of this PolledQueryCalendarSchedule
	 * @param days the new filter field value for the days field
	 * @return this PolledQueryCalendarSchedule
	 * @throws ScheduleFormatException if the days value is illegal
	 */
	public PolledQueryCalendarSchedule setDays(String days) throws ScheduleFormatException{
		validate(days, "days", 1, 31);
		this.days = days;
		return this;
	}

	/**
	 * set the months filter field of this PolledQueryCalendarSchedule
	 * @param months the new filter field value for the months field
	 * @return this PolledQueryCalendarSchedule
	 * @throws ScheduleFormatException if the months value is illegal
	 */
	public PolledQueryCalendarSchedule setMonths(String months) throws ScheduleFormatException{
		validate(months, "months", 1, 12);
		this.months = months;
		return this;
	}

	/**
	 * set the weekdays filter field of this PolledQueryCalendarSchedule
	 * @param weekdays the new filter field value for the weekdays field
	 * @return this PolledQueryCalendarSchedule
	 * @throws ScheduleFormatException if the weekdays value is illegal
	 */
	public PolledQueryCalendarSchedule setWeekdays(String weekdays) throws ScheduleFormatException{
		validate(weekdays, "weekdays", 0, 6);
		this.weekdays = weekdays;
		return this;
	}

	/**
	 * set the timezone of this PolledQueryCalendarSchedule
	 * @param timezoneName the new timezoneName name 
	 * @return this PolledQueryCalendarSchedule
	 * @throws ScheduleFormatException if the timezone name  is illegal
	 * The implementation currently does not check the validity of the timezone name!
	 */
	public PolledQueryCalendarSchedule setTimezoneName(String timezoneName) throws ScheduleFormatException{
		this.timezoneName = timezoneName;
		return this;
	}
	/**
	 * set the timezone this PolledQueryCalendarSchedule, based on a java.util.TimeZone instance
	 * @param tz the TimeZone to use for obtaining a timezone name  
	 * @return this PolledQueryCalendarSchedule
	 */
	public PolledQueryCalendarSchedule setTimezoneName(TimeZone tz){
		this.timezoneName = tz.getID();
		return this;
	}
	
	private static int validate1 (String trimmed, String units, int min, int max) throws ScheduleFormatException{
		// trimmed has no leading or trailing whitespace
		if (trimmed.isEmpty())
			throw new ScheduleFormatException("the empty string is not a valid decimal integer", null);
		try {
			var unitVal = Integer.parseUnsignedInt(trimmed);
			if (unitVal >= min && unitVal <= max) return unitVal;
			throw new ScheduleFormatException(
					String.format("%d is not a legitimate value for %s", unitVal, units), null);
		}catch (NumberFormatException nf) {
			throw new ScheduleFormatException(String.format("<%s> is not a valid decimal integer", trimmed), nf);
		}
	}

	private static void validate(String value, String units, int min, int max) throws ScheduleFormatException {
		for (var spec : value.split(",")) {
			spec = spec.trim();
			if (spec == "*") continue;
			var hyphenLoc = spec.indexOf('-');
				if (hyphenLoc == -1) {
					validate1(spec, units, min, max);
				} else {// a range
					var lb = validate1(spec.substring(0, hyphenLoc).stripTrailing(), units, min, max);
					var ub = validate1(spec.substring(hyphenLoc+1).stripLeading(), units, min, max);
					if (lb > ub)
						throw new ScheduleFormatException(
								String.format("%s is in invalid range", spec), null);
				}
		}
	}
	
	/*
	 (make-instance 'scheduling:calendar-schedule
	      :minutes (decode-time-unit-string json "minutes" "0" 0 59)
	      :hours (decode-time-unit-string json "hours" "0" 0 23)
	      :days (decode-time-unit-string json "days" "*" 1 31)
	      :months (decode-time-unit-string json "months" "*" 1 12)
	      :weekdays (decode-time-unit-string json "weekdays" "*" 0 6)
	      :timezone tzone)
	 */
	@Override
	public JsonNode asJson() {
		var jo = JsonNodeFactory.instance.objectNode();
		if (minutes != "0") jo.put("minutes", minutes);
		if (hours != "0") jo.put("hours", hours);
		if (days != "*") jo.put("days", days);
		if (months != "*") jo.put("months", months);
		if (weekdays != "*") jo.put("weekdays", weekdays);
		jo.put("timezond", timezoneName);
		return jo;
	}
}
