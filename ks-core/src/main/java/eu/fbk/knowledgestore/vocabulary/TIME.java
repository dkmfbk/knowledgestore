package eu.fbk.knowledgestore.vocabulary;

import org.openrdf.model.Namespace;
import org.openrdf.model.URI;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * Constants for the OWL Time.
 * 
 * @see <a href="http://dkhttp://www.w3.org/2006/time">vocabulary specification</a>
 */
public final class TIME {

    /** Recommended prefix for the vocabulary namespace: "time". */
    public static final String PREFIX = "time";

    /** Vocabulary namespace: "http://www.w3.org/2006/time#". */
    public static final String NAMESPACE = "http://www.w3.org/2006/time#";

    /** Immutable {@link Namespace} constant for the vocabulary namespace. */
    public static final Namespace NS = new NamespaceImpl(PREFIX, NAMESPACE);

    // CLASSES

    /** Class time:DateTimeDescription. */
    public static final URI DATE_TIME_DESCRIPTION = createURI("DateTimeDescription");

    /** Class time:DateTimeInterval. */
    public static final URI DATE_TIME_INTERVAL = createURI("DateTimeInterval");

    /** Class time:DayOfWeek. */
    public static final URI DAY_OF_WEEK_ENUM = createURI("DayOfWeek");

    /** Class time:DurationDescription. */
    public static final URI DURATION_DESCRIPTION = createURI("DurationDescription");

    /** Class time:Instant. */
    public static final URI INSTANT = createURI("Instant");

    /** Class time:Interval. */
    public static final URI INTERVAL = createURI("Interval");

    /** Class time:ProperInterval. */
    public static final URI PROPER_INTERVAL = createURI("ProperInterval");

    /** Class time:TemporalEntity. */
    public static final URI TEMPORAL_ENTITY = createURI("TemporalEntity");

    /** Class time:TemporalUnit. */
    public static final URI TEMPORAL_UNIT = createURI("TemporalUnit");

    // PROPERTIES

    /** Property time:after. */
    public static final URI AFTER = createURI("after");

    /** Property time:before. */
    public static final URI BEFORE = createURI("before");

    /** Property time:day. */
    public static final URI DAY = createURI("day");

    /** Property time:dayOfWeek. */
    public static final URI DAY_OF_WEEK = createURI("dayOfWeek");

    /** Property time:dayOfYear. */
    public static final URI DAY_OF_YEAR = createURI("dayOfYear");

    /** Property time:days. */
    public static final URI DAYS = createURI("days");

    /** Property time:hasBeginning. */
    public static final URI HAS_BEGINNING = createURI("hasBeginning");

    /** Property time:hasDateTimeDescription. */
    public static final URI HAS_DATE_TIME_DESCRIPTION = createURI("hasDateTimeDescription");

    /** Property time:hasDurationDescription. */
    public static final URI HAS_DURATION_DESCRIPTION = createURI("hasDurationDescription");

    /** Property time:hasEnd. */
    public static final URI HAS_END = createURI("hasEnd");

    /** Property time:hour. */
    public static final URI HOUR = createURI("hour");

    /** Property time:hours. */
    public static final URI HOURS = createURI("hours");

    /** Property time:inDateTime. */
    public static final URI IN_DATE_TIME = createURI("inDateTime");

    /** Property time:inXSDDateTime. */
    public static final URI IN_XSDDATE_TIME = createURI("inXSDDateTime");

    /** Property time:inside. */
    public static final URI INSIDE = createURI("inside");

    /** Property time:intervalAfter. */
    public static final URI INTERVAL_AFTER = createURI("intervalAfter");

    /** Property time:intervalBefore. */
    public static final URI INTERVAL_BEFORE = createURI("intervalBefore");

    /** Property time:intervalContains. */
    public static final URI INTERVAL_CONTAINS = createURI("intervalContains");

    /** Property time:intervalDuring. */
    public static final URI INTERVAL_DURING = createURI("intervalDuring");

    /** Property time:intervalEquals. */
    public static final URI INTERVAL_EQUALS = createURI("intervalEquals");

    /** Property time:intervalFinishedBy. */
    public static final URI INTERVAL_FINISHED_BY = createURI("intervalFinishedBy");

    /** Property time:intervalFinishes. */
    public static final URI INTERVAL_FINISHES = createURI("intervalFinishes");

    /** Property time:intervalMeets. */
    public static final URI INTERVAL_MEETS = createURI("intervalMeets");

    /** Property time:intervalMetBy. */
    public static final URI INTERVAL_MET_BY = createURI("intervalMetBy");

    /** Property time:intervalOverlappedBy. */
    public static final URI INTERVAL_OVERLAPPED_BY = createURI("intervalOverlappedBy");

    /** Property time:intervalOverlaps. */
    public static final URI INTERVAL_OVERLAPS = createURI("intervalOverlaps");

    /** Property time:intervalStartedBy. */
    public static final URI INTERVAL_STARTED_BY = createURI("intervalStartedBy");

    /** Property time:intervalStarts. */
    public static final URI INTERVAL_STARTS = createURI("intervalStarts");

    /** Property time:minute. */
    public static final URI MINUTE = createURI("minute");

    /** Property time:minutes. */
    public static final URI MINUTES = createURI("minutes");

    /** Property time:month. */
    public static final URI MONTH = createURI("month");

    /** Property time:months. */
    public static final URI MONTHS = createURI("months");

    /** Property time:second. */
    public static final URI SECOND = createURI("second");

    /** Property time:seconds. */
    public static final URI SECONDS = createURI("seconds");

    /** Property time:timeZone. */
    public static final URI TIME_ZONE = createURI("timeZone");

    /** Property time:unitType. */
    public static final URI UNIT_TYPE = createURI("unitType");

    /** Property time:week. */
    public static final URI WEEK = createURI("week");

    /** Property time:weeks. */
    public static final URI WEEKS = createURI("weeks");

    /** Property time:xsdDateTime. */
    public static final URI XSD_DATE_TIME = createURI("xsdDateTime");

    /** Property time:year. */
    public static final URI YEAR = createURI("year");

    /** Property time:years. */
    public static final URI YEARS = createURI("years");

    // INDIVIDUALS

    /** Individual time:Friday. */
    public static final URI FRIDAY = createURI("Friday");

    /** Individual time:Monday. */
    public static final URI MONDAY = createURI("Monday");

    /** Individual time:Saturday. */
    public static final URI SATURDAY = createURI("Saturday");

    /** Individual time:Sunday. */
    public static final URI SUNDAY = createURI("Sunday");

    /** Individual time:Thursday. */
    public static final URI THURSDAY = createURI("Thursday");

    /** Individual time:Tuesday. */
    public static final URI TUESDAY = createURI("Tuesday");

    /** Individual time:Wednesday. */
    public static final URI WEDNESDAY = createURI("Wednesday");

    /** Individual time:unitDay. */
    public static final URI UNIT_DAY = createURI("unitDay");

    /** Individual time:unitHour. */
    public static final URI UNIT_HOUR = createURI("unitHour");

    /** Individual time:unitMinute. */
    public static final URI UNIT_MINUTE = createURI("unitMinute");

    /** Individual time:unitMonth. */
    public static final URI UNIT_MONTH = createURI("unitMonth");

    /** Individual time:unitSecond. */
    public static final URI UNIT_SECOND = createURI("unitSecond");

    /** Individual time:unitWeek. */
    public static final URI UNIT_WEEK = createURI("unitWeek");

    /** Individual time:unitYear. */
    public static final URI UNIT_YEAR = createURI("unitYear");

    // HELPER METHODS

    private static URI createURI(final String localName) {
        return ValueFactoryImpl.getInstance().createURI(NAMESPACE, localName);
    }

    private TIME() {
    }

}
