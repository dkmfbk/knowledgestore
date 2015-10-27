package eu.fbk.knowledgestore.vocabulary;

import org.openrdf.model.Namespace;
import org.openrdf.model.URI;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * Constants for the NIF 2.0 Core Ontology.
 *
 * @see <a href="http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core/nif-core.html">
 *      vocabulary specification</a>
 */
public final class NIF {

    /** Recommended prefix for the vocabulary namespace: "nif". */
    public static final String PREFIX = "nif";

    /** Vocabulary namespace: "http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#". */
    public static final String NAMESPACE = "http://persistence.uni-leipzig.org"
            + "/nlp2rdf/ontologies/nif-core#";

    /** Immutable {@link Namespace} constant for the vocabulary namespace. */
    public static final Namespace NS = new NamespaceImpl(PREFIX, NAMESPACE);

    // CLASSES

    /** Class nif:Annotation. */
    public static final URI ANNOTATION_C = createURI("Annotation");

    /** Class nif:CollectionOccurrence. */
    public static final URI COLLECTION_OCCURRENCE = createURI("CollectionOccurrence");

    /** Class nif:ContextCollection. */
    public static final URI CONTEXT_COLLECTION = createURI("ContextCollection");

    /** Class nif:ContextHashBasedString. */
    public static final URI CONTEXT_HASH_BASED_STRING = createURI("ContextHashBasedString");

    /** Class nif:Context. */
    public static final URI CONTEXT = createURI("Context");

    /** Class nif:ContextOccurrence. */
    public static final URI CONTEXT_OCCURRENCE = createURI("ContextOccurrence");

    /** Class nif:CString. */
    public static final URI CSTRING = createURI("CString");

    /** Class nif:CStringInst. */
    public static final URI CSTRING_INST = createURI("CStringInst");

    /** Class nif:NormalizedCollectionOccurrence. */
    public static final URI NORMALIZED_COLLECTION_OCCURRENCE = //
    createURI("NormalizedCollectionOccurrence");

    /** Class nif:NormalizedContextOccurrence. */
    public static final URI NORMALIZED_CONTEXT_OCCURRENCE = //
    createURI("NormalizedContextOccurrence");

    /** Class nif:OffsetBasedString. */
    public static final URI OFFSET_BASED_STRING = createURI("OffsetBasedString");

    /** Class nif:Paragraph. */
    public static final URI PARAGRAPH = createURI("Paragraph");

    /** Class nif:Phrase. */
    public static final URI PHRASE = createURI("Phrase");

    /** Class nif:RFC5147String. */
    public static final URI RFC5147_STRING = createURI("RFC5147String");

    /** Class nif:Sentence. */
    public static final URI SENTENCE_C = createURI("Sentence");

    /** Class nif:String. */
    public static final URI STRING = createURI("String");

    /** Class nif:Structure. */
    public static final URI STRUCTURE = createURI("Structure");

    /** Class nif:Title. */
    public static final URI TITLE = createURI("Title");

    /** Class nif:URIScheme. */
    public static final URI URISCHEME = createURI("URIScheme");

    /** Class nif:Word. */
    public static final URI WORD_C = createURI("Word");

    // OBJECT PROPERTIES

    /** Property nif:annotation. */
    public static final URI ANNOTATION_P = createURI("annotation");

    /** Property nif:broaderContext. */
    public static final URI BROADER_CONTEXT = createURI("broaderContext");

    /** Property nif:dependency. */
    public static final URI DEPENDENCY = createURI("dependency");

    /** Property nif:dependencyTrans. */
    public static final URI DEPENDENCY_TRANS = createURI("dependencyTrans");

    /** Property nif:firstWord. */
    public static final URI FIRST_WORD = createURI("firstWord");

    /** Property nif:hasContext. */
    public static final URI HAS_CONTEXT = createURI("hasContext");

    /** Property nif:inter. */
    public static final URI INTER = createURI("inter");

    /** Property nif:lang. */
    public static final URI LANG = createURI("lang");

    /** Property nif:lastWord. */
    public static final URI LAST_WORD = createURI("lastWord");

    /** Property nif:narrowerContext. */
    public static final URI NARROWER_CONTEXT = createURI("narrowerContext");

    /** Property nif:nextSentence. */
    public static final URI NEXT_SENTENCE = createURI("nextSentence");

    /** Property nif:nextSentenceTrans. */
    public static final URI NEXT_SENTENCE_TRANS = createURI("nextSentenceTrans");

    /** Property nif:nextWord. */
    public static final URI NEXT_WORD = createURI("nextWord");

    /** Property nif:nextWordTrans. */
    public static final URI NEXT_WORD_TRANS = createURI("nextWordTrans");

    /** Property nif:oliaLink. */
    public static final URI OLIA_LINK = createURI("oliaLink");

    /** Property nif:oliaProv. */
    public static final URI OLIA_PROV = createURI("oliaProv");

    /** Property nif:opinion. */
    public static final URI OPINION = createURI("opinion");

    /** Property nif:predLang. */
    public static final URI PRED_LANG = createURI("predLang");

    /** Property nif:previousSentence. */
    public static final URI PREVIOUS_SENTENCE = createURI("previousSentence");

    /** Property nif:previousSentenceTrans. */
    public static final URI PREVIOUS_SENTENCE_TRANS = createURI("previousSentenceTrans");

    /** Property nif:previousWord. */
    public static final URI PREVIOUS_WORD = createURI("previousWord");

    /** Property nif:previousWordTrans. */
    public static final URI PREVIOUS_WORD_TRANS = createURI("previousWordTrans");

    /** Property nif:referenceContext. */
    public static final URI REFERENCE_CONTEXT = createURI("referenceContext");

    /** Property nif:sentence. */
    public static final URI SENTENCE_P = createURI("sentence");

    /** Property nif:sourceUrl. */
    public static final URI SOURCE_URL = createURI("sourceUrl");

    /** Property nif:subString. */
    public static final URI SUB_STRING = createURI("subString");

    /** Property nif:subStringTrans. */
    public static final URI SUB_STRING_TRANS = createURI("subStringTrans");

    /** Property nif:superString. */
    public static final URI SUPER_STRING = createURI("superString");

    /** Property nif:superStringTrans. */
    public static final URI SUPER_STRING_TRANS = createURI("superStringTrans");

    /** Property nif:wasConvertedFrom. */
    public static final URI WAS_CONVERTED_FROM = createURI("wasConvertedFrom");

    /** Property nif:word. */
    public static final URI WORD_P = createURI("word");

    // DATATYPE PROPERTIES

    /** Property nif:after. */
    public static final URI AFTER = createURI("after");

    /** Property nif:anchorOf. */
    public static final URI ANCHOR_OF = createURI("anchorOf");

    /** Property nif:before. */
    public static final URI BEFORE = createURI("before");

    /** Property nif:beginIndex. */
    public static final URI BEGIN_INDEX = createURI("beginIndex");

    /** Property nif:confidence. */
    public static final URI CONFIDENCE = createURI("confidence");

    /** Property nif:dependencyRelationType. */
    public static final URI DEPENDENCY_RELATION_TYPE = createURI("dependencyRelationType");

    /** Property nif:endIndex. */
    public static final URI END_INDEX = createURI("endIndex");

    /** Property nif:head. */
    public static final URI HEAD = createURI("head");

    /** Property nif:isString. */
    public static final URI IS_STRING = createURI("isString");

    /** Property nif:keyword. */
    public static final URI KEYWORD = createURI("keyword");

    /** Property nif:lemma. */
    public static final URI LEMMA = createURI("lemma");

    /** Property nif:oliaConf. */
    public static final URI OLIA_CONF = createURI("oliaConf");

    /** Property nif:posTag. */
    public static final URI POS_TAG = createURI("posTag");

    /** Property nif:sentimentValue. */
    public static final URI SENTIMENT_VALUE = createURI("sentimentValue");

    /** Property nif:stem. */
    public static final URI STEM = createURI("stem");

    /** Property nif:topic. */
    public static final URI TOPIC = createURI("topic");

    // ANNOTATION PROPERTIES

    /** Property nif:category. */
    public static final URI CATEGORY = createURI("category");

    /** Property nif:classAnnotation. */
    public static final URI CLASS_ANNOTATION = createURI("classAnnotation");

    /** Property nif:literalAnnotation. */
    public static final URI LITERAL_ANNOTATION = createURI("literalAnnotation");

    /** Property nif:oliaCategory. */
    public static final URI OLIA_CATEGORY = createURI("oliaCategory");

    /** Property nif:taMsClassRef. */
    public static final URI TA_MS_CLASS_REF = createURI("taMsClassRef");

    // HELPER METHODS

    private static URI createURI(final String localName) {
        return ValueFactoryImpl.getInstance().createURI(NAMESPACE, localName);
    }

    private NIF() {
    }

}
