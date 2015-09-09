package eu.fbk.knowledgestore.vocabulary;

import org.openrdf.model.Namespace;
import org.openrdf.model.URI;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * Constants for the NewsReader Data Model.
 * 
 * @see <a href="http://dkm.fbk.eu/ontologies/newsreader">vocabulary specification</a>
 */
public final class NWR {

    /** Recommended prefix for the vocabulary namespace: "nwr". */
    public static final String PREFIX = "nwr";

    /** Vocabulary namespace: "http://dkm.fbk.eu/ontologies/newsreader#". */
    public static final String NAMESPACE = "http://dkm.fbk.eu/ontologies/newsreader#";

    /** Immutable {@link Namespace} constant for the vocabulary namespace. */
    public static final Namespace NS = new NamespaceImpl(PREFIX, NAMESPACE);

    // CLASSES

    /** Class nwr:Aspect. */
    public static final URI ASPECT_ENUM = createURI("Aspect");

    /** Class nwr:CLink. */
    public static final URI CLINK = createURI("CLink");

    /** Class nwr:CSignalMention. */
    public static final URI CSIGNAL_MENTION = createURI("CSignalMention");

    /** Class nwr:Certainty. */
    public static final URI CERTAINTY_ENUM = createURI("Certainty");

    /** Class nwr:EntityClass. */
    public static final URI ENTITY_CLASS_ENUM = createURI("EntityClass");

    /** Class nwr:EntityMention. */
    public static final URI ENTITY_MENTION = createURI("EntityMention");

    /** Class nwr:EntityType. */
    public static final URI ENTITY_TYPE_ENUM = createURI("EntityType");

    /** Class nwr:EventClass. */
    public static final URI EVENT_CLASS_ENUM = createURI("EventClass");

    /** Class nwr:EventMention. */
    public static final URI EVENT_MENTION = createURI("EventMention");

    /** Class nwr:Factuality. */
    public static final URI FACTUALITY_ENUM = createURI("Factuality");

    /** Class nwr:Factuality. */
    public static final URI FACTBANK = createURI("factbank");

    /** Class nwr:Factuality. */
    public static final URI ATTRIBUTION_TENSE = createURI("attributionTense");

    /** Class nwr:Factuality. */
    public static final URI ATTRIBUTION_CERTAINTY = createURI("attributionCertainty");

    /** Class nwr:Factuality. */
    public static final URI ATTRIBUTION_POLARITY = createURI("attributionPolarity");

    /** Class nwr:FunctionInDocument. */
    public static final URI FUNCTION_IN_DOCUMENT_ENUM = createURI("FunctionInDocument");

    /** Class nwr:GLink. */
    public static final URI GLINK = createURI("GLink");

    /** Class nwr:NAFDocument. */
    public static final URI NAFDOCUMENT = createURI("NAFDocument");

    /** Class nwr:NAFLayer. */
    public static final URI NAFLAYER_ENUM = createURI("NAFLayer");

    /** Class nwr:NAFProcessor. */
    public static final URI NAFPROCESSOR = createURI("NAFProcessor");

    /** Class nwr:News. */
    public static final URI NEWS = createURI("News");

    /** Class nwr:ObjectMention. */
    public static final URI OBJECT_MENTION = createURI("ObjectMention");

    /** Class nwr:PartOfSpeech. */
    public static final URI PART_OF_SPEECH_ENUM = createURI("PartOfSpeech");

    /** Class nwr:Participation. */
    public static final URI PARTICIPATION = createURI("Participation");

    /** Class nwr:Polarity. */
    public static final URI POLARITY_ENUM = createURI("Polarity");

    /** Class nwr:RelationMention. */
    public static final URI RELATION_MENTION = createURI("RelationMention");

    /** Class nwr:SLink. */
    public static final URI SLINK = createURI("SLink");

    /** Class nwr:SignalMention. */
    public static final URI SIGNAL_MENTION = createURI("SignalMention");

    /** Class nwr:SyntacticType. */
    public static final URI SYNTACTIC_TYPE_ENUM = createURI("SyntacticType");

    /** Class nwr:TIMEX3Modifier. */
    public static final URI TIMEX3_MODIFIER_ENUM = createURI("TIMEX3Modifier");

    /** Class nwr:TIMEX3Type. */
    public static final URI TIMEX3_TYPE_ENUM = createURI("TIMEX3Type");

    /** Class nwr:TLink. */
    public static final URI TLINK = createURI("TLink");

    /** Class nwr:TLinkType. */
    public static final URI TLINK_TYPE_ENUM = createURI("TLinkType");

    /** Class nwr:Tense. */
    public static final URI TENSE_ENUM = createURI("Tense");

    /** Class nwr:TimeMention. */
    public static final URI TIME_MENTION = createURI("TimeMention");

    /** Class nwr:TimeOrEventMention. */
    public static final URI TIME_OR_EVENT_MENTION = createURI("TimeOrEventMention");

    /** Class nwr:ValueMention. */
    public static final URI VALUE_MENTION = createURI("ValueMention");

    /** Class nwr:ValueType. */
    public static final URI VALUE_TYPE_ENUM = createURI("ValueType");

    // PROPERTIES
    
    /** Property nwr:section. */
    public static final URI SECTION = createURI("section");
    
    /** Property nwr:magazine. */
    public static final URI MAGAZINE = createURI("magazine");

    /** Property nwr:location. */
    public static final URI LOCATION = createURI("location");
    
    /** Property nwr:publisher. */
    public static final URI PUBLISHER = createURI("publisher");

    /** Property nwr:anchorTime. */
    public static final URI ANCHOR_TIME = createURI("anchorTime");

    /** Property nwr:annotatedWith. */
    public static final URI ANNOTATED_WITH = createURI("annotatedWith");

    /** Property nwr:annotationOf. */
    public static final URI ANNOTATION_OF = createURI("annotationOf");

    /** Property nwr:aspect. */
    public static final URI ASPECT = createURI("aspect");

    /** Property nwr:beginPoint. */
    public static final URI BEGIN_POINT = createURI("beginPoint");

    /** Property nwr:certainty. */
    public static final URI CERTAINTY = createURI("certainty");

    /** Property nwr:confidence. */
    public static final URI CONFIDENCE = createURI("confidence");

    /** Property nwr:crystallized. */
    public static final URI CRYSTALLIZED = createURI("crystallized");

    /** Property nwr:csignal. */
    public static final URI CSIGNAL = createURI("csignal");

    /** Property nwr:endPoint. */
    public static final URI END_POINT = createURI("endPoint");

    /** Property nwr:entityClass. */
    public static final URI ENTITY_CLASS = createURI("entityClass");

    /** Property nwr:entityType. */
    public static final URI ENTITY_TYPE = createURI("entityType");

    /** Property nwr:eventClass. */
    public static final URI EVENT_CLASS = createURI("eventClass");

    /** Property nwr:factuality. */
    public static final URI FACTUALITY = createURI("factuality");

    /** Property nwr:factualityConfidence. */
    public static final URI FACTUALITY_CONFIDENCE = createURI("factualityConfidence");
    
    /** Property nwr:factBank. */
    public static final URI FACT_BANK = createURI("factBank");
    

    /** Property nwr:framenetRef. */
    public static final URI FRAMENET_REF = createURI("framenetRef");

    /** Property nwr:freq. */
    public static final URI FREQ = createURI("freq");

    /** Property nwr:functionInDocument. */
    public static final URI FUNCTION_IN_DOCUMENT = createURI("functionInDocument");

    /** Property nwr:layer. */
    public static final URI LAYER = createURI("layer");

    /** Property nwr:localCorefID. */
    public static final URI LOCAL_COREF_ID = createURI("localCorefID");

    /** Property nwr:mod. */
    public static final URI MOD = createURI("mod");

    /** Property nwr:modality. */
    public static final URI MODALITY = createURI("modality");

    /** Property nwr:nombankRef. */
    public static final URI NOMBANK_REF = createURI("nombankRef");

    /** Property nwr:esoRef. */
    public static final URI ESO_REF = createURI("esoRef");

    /** Property nwr:originalFileFormat. */
    public static final URI ORIGINAL_FILE_FORMAT = createURI("originalFileFormat");

    /** Property nwr:originalFileName. */
    public static final URI ORIGINAL_FILE_NAME = createURI("originalFileName");

    /** Property nwr:originalPages. */
    public static final URI ORIGINAL_PAGES = createURI("originalPages");

    /** Property nwr:polarity. */
    public static final URI POLARITY = createURI("polarity");

    /** Property nwr:pos. */
    public static final URI POS = createURI("pos");

    /** Property nwr:pred. */
    public static final URI PRED = createURI("pred");

    /** Property nwr:propbankRef. */
    public static final URI PROPBANK_REF = createURI("propbankRef");

    /** Property nwr:quant. */
    public static final URI QUANT = createURI("quant");

    /** Property nwr:relType. */
    public static final URI REL_TYPE = createURI("relType");

    /** Property nwr:signal. */
    public static final URI SIGNAL = createURI("signal");

    /** Property nwr:source. */
    public static final URI SOURCE = createURI("source");

    /** Property nwr:syntacticHead. */
    public static final URI SYNTACTIC_HEAD = createURI("syntacticHead");

    /** Property nwr:syntacticType. */
    public static final URI SYNTACTIC_TYPE = createURI("syntacticType");

    /** Property nwr:target. */
    public static final URI TARGET = createURI("target");

    /** Property nwr:temporalFunction. */
    public static final URI TEMPORAL_FUNCTION = createURI("temporalFunction");

    /** Property nwr:tense. */
    public static final URI TENSE = createURI("tense");

    /** Property nwr:termID. */
    public static final URI TERM_ID = createURI("termID");

    /** Property nwr:thematicRole. */
    public static final URI THEMATIC_ROLE = createURI("thematicRole");

    /** Property nwr:timeType. */
    public static final URI TIME_TYPE = createURI("timeType");

    /** Property nwr:value. */
    public static final URI VALUE = createURI("value");

    /** Property nwr:valueFromFunction. */
    public static final URI VALUE_FROM_FUNCTION = createURI("valueFromFunction");

    /** Property nwr:valueType. */
    public static final URI VALUE_TYPE = createURI("valueType");

    /** Property nwr:verbnetRef. */
    public static final URI VERBNET_REF = createURI("verbnetRef");

    /** Property nwr:version. */
    public static final URI VERSION = createURI("version");

    // INDIVIDUALS

    /** Individual nwr:aspect_none. */
    public static final URI ASPECT_NONE = createURI("aspect_none");

    /** Individual nwr:aspect_perfective. */
    public static final URI ASPECT_PERFECTIVE = createURI("aspect_perfective");

    /** Individual nwr:aspect_perfective_progressive. */
    public static final URI ASPECT_PERFECTIVE_PROGRESSIVE = createURI(//
    "aspect_perfective_progressive");

    /** Individual nwr:aspect_progressive. */
    public static final URI ASPECT_PROGRESSIVE = createURI("aspect_progressive");

    /** Individual nwr:certain. */
    public static final URI CERTAIN = createURI("certain");

    /** Individual nwr:counterfactual. */
    public static final URI COUNTERFACTUAL = createURI("counterfactual");

    /** Individual nwr:entity_class_gen. */
    public static final URI ENTITY_CLASS_GEN = createURI("entity_class_gen");

    /** Individual nwr:entity_class_neg. */
    public static final URI ENTITY_CLASS_NEG = createURI("entity_class_neg");

    /** Individual nwr:entity_class_spc. */
    public static final URI ENTITY_CLASS_SPC = createURI("entity_class_spc");

    /** Individual nwr:entity_class_usp. */
    public static final URI ENTITY_CLASS_USP = createURI("entity_class_usp");

    /** Individual nwr:entity_type_fin. */
    public static final URI ENTITY_TYPE_FIN = createURI("entity_type_fin");

    /** Individual nwr:entity_type_loc. */
    public static final URI ENTITY_TYPE_LOC = createURI("entity_type_loc");

    /** Individual nwr:entity_type_mix. */
    public static final URI ENTITY_TYPE_MIX = createURI("entity_type_mix");

    /** Individual nwr:entity_type_org. */
    public static final URI ENTITY_TYPE_ORG = createURI("entity_type_org");

    /** Individual nwr:entity_type_per. */
    public static final URI ENTITY_TYPE_PER = createURI("entity_type_per");

    /** Individual nwr:entity_type_pro. */
    public static final URI ENTITY_TYPE_PRO = createURI("entity_type_pro");

    /** Individual nwr:event_grammatical. */
    public static final URI EVENT_GRAMMATICAL = createURI("event_grammatical");

    /** Individual nwr:event_other. */
    public static final URI EVENT_OTHER = createURI("event_other");

    /** Individual nwr:event_speech_cognitive. */
    public static final URI EVENT_SPEECH_COGNITIVE = createURI("event_speech_cognitive");

    /** Individual nwr:factual. */
    public static final URI FACTUAL = createURI("factual");

    /** Individual nwr:function_creation_time. */
    public static final URI FUNCTION_CREATION_TIME = createURI("function_creation_time");

    /** Individual nwr:function_expiration_time. */
    public static final URI FUNCTION_EXPIRATION_TIME = createURI("function_expiration_time");

    /** Individual nwr:function_modification_time. */
    public static final URI FUNCTION_MODIFICATION_TIME = createURI("function_modification_time");

    /** Individual nwr:function_none. */
    public static final URI FUNCTION_NONE = createURI("function_none");

    /** Individual nwr:function_publication_time. */
    public static final URI FUNCTION_PUBLICATION_TIME = createURI("function_publication_time");

    /** Individual nwr:function_reception_time. */
    public static final URI FUNCTION_RECEPTION_TIME = createURI("function_reception_time");

    /** Individual nwr:function_release_time. */
    public static final URI FUNCTION_RELEASE_TIME = createURI("function_release_time");

    /** Individual nwr:layer_chunks. */
    public static final URI LAYER_CHUNKS = createURI("layer_chunks");

    /** Individual nwr:layer_constituency. */
    public static final URI LAYER_CONSTITUENCY = createURI("layer_constituency");

    /** Individual nwr:layer_coreferences. */
    public static final URI LAYER_COREFERENCES = createURI("layer_coreferences");

    /** Individual nwr:layer_deps. */
    public static final URI LAYER_DEPS = createURI("layer_deps");

    /** Individual nwr:layer_entities. */
    public static final URI LAYER_ENTITIES = createURI("layer_entities");

    /** Individual nwr:layer_factuality. */
    public static final URI LAYER_FACTUALITY = createURI("layer_factuality");

    /** Individual nwr:layer_raw. */
    public static final URI LAYER_RAW = createURI("layer_raw");

    /** Individual nwr:layer_srl. */
    public static final URI LAYER_SRL = createURI("layer_srl");

    /** Individual nwr:layer_terms. */
    public static final URI LAYER_TERMS = createURI("layer_terms");

    /** Individual nwr:layer_text. */
    public static final URI LAYER_TEXT = createURI("layer_text");

    /** Individual nwr:layer_time_expressions. */
    public static final URI LAYER_TIME_EXPRESSIONS = createURI("layer_time_expressions");

    /** Individual nwr:layer_time_expressions. */
    public static final URI LAYER_TOPICS = createURI("layer_topics");
    
    /** Individual nwr:layer_time_expressions. */
    public static final URI LAYER_MARKABLES = createURI("layer_markables");
    
    /** Individual nwr:layer_time_expressions. */
    public static final URI LAYER_FACTUALITIES = createURI("layer_factualities");
    
    /** Individual nwr:layer_opinions. */
    public static final URI LAYER_OPINIONS = createURI("layer_opinions");

    /** Individual nwr:layer_temporal_relations. */
    public static final URI LAYER_TEMPORAL_RELATIONS = createURI("layer_temporal_relations");

    /** Individual nwr:layer_causal_relations. */
    public static final URI LAYER_CAUSAL_RELATIONS = createURI("layer_causal_relations");

    /** Individual nwr:layer_vua_multiword_tagger. */
    public static final URI LAYER_VUA_MULTIWORD_TAGGER = createURI("layer_vua_multiword_tagger");

    /** Individual nwr:layer_vua_event_coref_intradoc_lemma_baseline. */
    public static final URI LAYER_VUA_EVENT_COREF_INTRADOC_LEMMA_BASELINE = createURI("layer_vua_event_coref_intradoc_lemma_baseline");

    /** Individual nwr:mod_after. */
    public static final URI MOD_AFTER = createURI("mod_after");

    /** Individual nwr:mod_approx. */
    public static final URI MOD_APPROX = createURI("mod_approx");

    /** Individual nwr:mod_before. */
    public static final URI MOD_BEFORE = createURI("mod_before");

    /** Individual nwr:mod_end. */
    public static final URI MOD_END = createURI("mod_end");

    /** Individual nwr:mod_equal_or_less. */
    public static final URI MOD_EQUAL_OR_LESS = createURI("mod_equal_or_less");

    /** Individual nwr:mod_equal_or_more. */
    public static final URI MOD_EQUAL_OR_MORE = createURI("mod_equal_or_more");

    /** Individual nwr:mod_less_than. */
    public static final URI MOD_LESS_THAN = createURI("mod_less_than");

    /** Individual nwr:mod_mid. */
    public static final URI MOD_MID = createURI("mod_mid");

    /** Individual nwr:mod_more_than. */
    public static final URI MOD_MORE_THAN = createURI("mod_more_than");

    /** Individual nwr:mod_on_or_after. */
    public static final URI MOD_ON_OR_AFTER = createURI("mod_on_or_after");

    /** Individual nwr:mod_on_or_before. */
    public static final URI MOD_ON_OR_BEFORE = createURI("mod_on_or_before");

    /** Individual nwr:mod_start. */
    public static final URI MOD_START = createURI("mod_start");

    /** Individual nwr:non_factual. */
    public static final URI NON_FACTUAL = createURI("non_factual");

    /** Individual nwr:polarity_neg. */
    public static final URI POLARITY_NEG = createURI("polarity_neg");

    /** Individual nwr:polarity_pos. */
    public static final URI POLARITY_POS = createURI("polarity_pos");

    /** Individual nwr:pos_noun. */
    public static final URI POS_NOUN = createURI("pos_noun");

    /** Individual nwr:pos_other. */
    public static final URI POS_OTHER = createURI("pos_other");

    /** Individual nwr:pos_verb. */
    public static final URI POS_VERB = createURI("pos_verb");

    /** Individual nwr:syntactic_type_app. */
    public static final URI SYNTACTIC_TYPE_APP = createURI("syntactic_type_app");

    /** Individual nwr:syntactic_type_arc. */
    public static final URI SYNTACTIC_TYPE_ARC = createURI("syntactic_type_arc");

    /** Individual nwr:syntactic_type_conj. */
    public static final URI SYNTACTIC_TYPE_CONJ = createURI("syntactic_type_conj");

    /** Individual nwr:syntactic_type_hls. */
    public static final URI SYNTACTIC_TYPE_HLS = createURI("syntactic_type_hls");

    /** Individual nwr:syntactic_type_nam. */
    public static final URI SYNTACTIC_TYPE_NAM = createURI("syntactic_type_nam");

    /** Individual nwr:syntactic_type_nom. */
    public static final URI SYNTACTIC_TYPE_NOM = createURI("syntactic_type_nom");

    /** Individual nwr:syntactic_type_pre. */
    public static final URI SYNTACTIC_TYPE_PRE = createURI("syntactic_type_pre");

    /** Individual nwr:syntactic_type_pro. */
    public static final URI SYNTACTIC_TYPE_PRO = createURI("syntactic_type_pro");

    /** Individual nwr:syntactic_type_ptv. */
    public static final URI SYNTACTIC_TYPE_PTV = createURI("syntactic_type_ptv");

    /** Individual nwr:tense_future. */
    public static final URI TENSE_FUTURE = createURI("tense_future");

    /** Individual nwr:tense_infinitive. */
    public static final URI TENSE_INFINITIVE = createURI("tense_infinitive");

    /** Individual nwr:tense_none. */
    public static final URI TENSE_NONE = createURI("tense_none");

    /** Individual nwr:tense_past. */
    public static final URI TENSE_PAST = createURI("tense_past");

    /** Individual nwr:tense_pastpart. */
    public static final URI TENSE_PASTPART = createURI("tense_pastpart");

    /** Individual nwr:tense_present. */
    public static final URI TENSE_PRESENT = createURI("tense_present");

    /** Individual nwr:tense_prespart. */
    public static final URI TENSE_PRESPART = createURI("tense_prespart");

    /** Individual nwr:timex3_date. */
    public static final URI TIMEX3_DATE = createURI("timex3_date");

    /** Individual nwr:timex3_duration. */
    public static final URI TIMEX3_DURATION = createURI("timex3_duration");

    /** Individual nwr:timex3_set. */
    public static final URI TIMEX3_SET = createURI("timex3_set");

    /** Individual nwr:timex3_time. */
    public static final URI TIMEX3_TIME = createURI("timex3_time");

    /** Individual nwr:tlink_from_tmx0. */
    public static final URI TLINK_FROM_TMX0 = createURI("tlink_from_tmx0");

    /** Individual nwr:tlink_tm_tmx0. */
    public static final URI TLINK_TO_TMX0 = createURI("tlink_to_tmx0");

    /** Individual nwr:tlink_after. */
    public static final URI TLINK_AFTER = createURI("tlink_after");

    /** Individual nwr:tlink_before. */
    public static final URI TLINK_BEFORE = createURI("tlink_before");

    /** Individual nwr:tlink_begins. */
    public static final URI TLINK_BEGINS = createURI("tlink_begins");

    /** Individual nwr:tlink_begun_by. */
    public static final URI TLINK_BEGUN_BY = createURI("tlink_begun_by");

    /** Individual nwr:tlink_ended_by. */
    public static final URI TLINK_ENDED_BY = createURI("tlink_ended_by");

    /** Individual nwr:tlink_ends. */
    public static final URI TLINK_ENDS = createURI("tlink_ends");

    /** Individual nwr:tlink_iafter. */
    public static final URI TLINK_IAFTER = createURI("tlink_iafter");

    /** Individual nwr:tlink_ibefore. */
    public static final URI TLINK_IBEFORE = createURI("tlink_ibefore");

    /** Individual nwr:tlink_includes. */
    public static final URI TLINK_INCLUDES = createURI("tlink_includes");

    /** Individual nwr:tlink_is_included. */
    public static final URI TLINK_IS_INCLUDED = createURI("tlink_is_included");

    /** Individual nwr:tlink_measure. */
    public static final URI TLINK_MEASURE = createURI("tlink_measure");

    /** Individual nwr:tlink_simultaneous. */
    public static final URI TLINK_SIMULTANEOUS = createURI("tlink_simultaneous");

    /** Individual nwr:uncertain. */
    public static final URI UNCERTAIN = createURI("uncertain");

    /** Individual nwr:value_money. */
    public static final URI VALUE_MONEY = createURI("value_money");

    /** Individual nwr:value_percent. */
    public static final URI VALUE_PERCENT = createURI("value_percent");

    /** Individual nwr:value_quantity. */
    public static final URI VALUE_QUANTITY = createURI("value_quantity");

    // HELPER METHODS

    private static URI createURI(final String localName) {
        return ValueFactoryImpl.getInstance().createURI(NAMESPACE, localName);
    }

    private NWR() {
    }

}
