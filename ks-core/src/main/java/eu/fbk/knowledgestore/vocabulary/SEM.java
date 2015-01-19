package eu.fbk.knowledgestore.vocabulary;

import org.openrdf.model.Namespace;
import org.openrdf.model.URI;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * Constants for the Simple Event Model (SEM) Ontology.
 * 
 * @see <a href="http://semanticweb.cs.vu.nl/2009/11/sem/">vocabulary specification</a>
 */
public final class SEM {

    /** Recommended prefix for the vocabulary namespace: "sem". */
    public static final String PREFIX = "sem";

    /** Vocabulary namespace: "http://semanticweb.cs.vu.nl/2009/11/sem/". */
    public static final String NAMESPACE = "http://semanticweb.cs.vu.nl/2009/11/sem/";

    /** Immutable {@link Namespace} constant for the vocabulary namespace. */
    public static final Namespace NS = new NamespaceImpl(PREFIX, NAMESPACE);

    // CLASSES

    /** Class sem:Actor. */
    public static final URI ACTOR = createURI("Actor");

    /** Class sem:ActorType. */
    public static final URI ACTOR_TYPE = createURI("ActorType");

    /** Class sem:Authority. */
    public static final URI AUTHORITY = createURI("Authority");

    /** Class sem:Constraint. */
    public static final URI CONSTRAINT = createURI("Constraint");

    /** Class sem:Core. */
    public static final URI CORE = createURI("Core");

    /** Class sem:Event. */
    public static final URI EVENT = createURI("Event");

    /** Class sem:EventType. */
    public static final URI EVENT_TYPE = createURI("EventType");

    /** Class sem:Object. */
    public static final URI OBJECT = createURI("Object");

    /** Class sem:Place. */
    public static final URI PLACE = createURI("Place");

    /** Class sem:PlaceType. */
    public static final URI PLACE_TYPE = createURI("PlaceType");

    /** Class sem:Role. */
    public static final URI ROLE = createURI("Role");

    /** Class sem:RoleType. */
    public static final URI ROLE_TYPE = createURI("RoleType");

    /** Class sem:Temporary. */
    public static final URI TEMPORARY = createURI("Temporary");

    /** Class sem:Time. */
    public static final URI TIME = createURI("Time");

    /** Class sem:TimeType. */
    public static final URI TIME_TYPE = createURI("TimeType");

    /** Class sem:Type. */
    public static final URI TYPE = createURI("Type");

    /** Class sem:View. */
    public static final URI VIEW = createURI("View");

    // PROPERTIES

    /** Property sem:accordingTo. */
    public static final URI ACCORDING_TO = createURI("accordingTo");

    /** Property sem:actorType. */
    public static final URI ACTOR_TYPE_PROPERTY = createURI("actorType");

    /** Property sem:eventProperty. */
    public static final URI EVENT_PROPERTY = createURI("eventProperty");

    /** Property sem:eventType. */
    public static final URI EVENT_TYPE_PROPERTY = createURI("eventType");

    /** Property sem:hasActor. */
    public static final URI HAS_ACTOR = createURI("hasActor");

    /** Property sem:hasBeginTimeStamp. */
    public static final URI HAS_BEGIN_TIME_STAMP = createURI("hasBeginTimeStamp");

    /** Property sem:hasEarliestBeginTimeStamp. */
    public static final URI HAS_EARLIEST_BEGIN_TIME_STAMP = createURI("hasEarliestBeginTimeStamp");

    /** Property sem:hasEarliestEndTimeStamp. */
    public static final URI HAS_EARLIEST_END_TIME_STAMP = createURI("hasEarliestEndTimeStamp");

    /** Property sem:hasEndTimeStamp. */
    public static final URI HAS_END_TIME_STAMP = createURI("hasEndTimeStamp");

    /** Property sem:hasLatestBeginTimeStamp. */
    public static final URI HAS_LATEST_BEGIN_TIME_STAMP = createURI("hasLatestBeginTimeStamp");

    /** Property sem:hasLatestEndTimeStamp. */
    public static final URI HAS_LATEST_END_TIME_STAMP = createURI("hasLatestEndTimeStamp");

    /** Property sem:hasPlace. */
    public static final URI HAS_PLACE = createURI("hasPlace");

    /** Property sem:hasSubEvent. */
    public static final URI HAS_SUB_EVENT = createURI("hasSubEvent");

    /** Property sem:hasSubType. */
    public static final URI HAS_SUB_TYPE = createURI("hasSubType");

    /** Property sem:hasTime. */
    public static final URI HAS_TIME = createURI("hasTime");

    /** Property sem:hasTimeStamp. */
    public static final URI HAS_TIME_STAMP = createURI("hasTimeStamp");

    /** Property sem:placeType. */
    public static final URI PLACE_TYPE_PROPERTY = createURI("placeType");

    /** Property sem:roleType. */
    public static final URI ROLE_TYPE_PROPERTY = createURI("roleType");

    /** Property sem:subEventOf. */
    public static final URI SUB_EVENT_OF = createURI("subEventOf");

    /** Property sem:subTypeOf. */
    public static final URI SUB_TYPE_OF = createURI("subTypeOf");

    /** Property sem:timeType. */
    public static final URI TIME_TYPE_PROPERTY = createURI("timeType");

    /** Property sem:type. */
    public static final URI TYPE_PROPERTY = createURI("type");

    // HELPER METHODS

    private static URI createURI(final String localName) {
        return ValueFactoryImpl.getInstance().createURI(NAMESPACE, localName);
    }

    private SEM() {
    }

}
