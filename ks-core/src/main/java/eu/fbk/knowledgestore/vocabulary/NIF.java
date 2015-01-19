package eu.fbk.knowledgestore.vocabulary;

import org.openrdf.model.Namespace;
import org.openrdf.model.URI;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * Constants for the NIF 2.0 Core Ontology (draft).
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

    /** Class nif:ArbitraryString. */
    public static final URI ARBITRARY_STRING = createURI("ArbitraryString");

    /** Class nif:CollectionOccurrence. */
    public static final URI COLLECTION_OCCURRENCE = createURI("CollectionOccurrence");

    /** Class nif:Context. */
    public static final URI CONTEXT = createURI("Context");

    /** Class nif:ContextHashBasedString. */
    public static final URI CONTEXT_HASH_BASED_STRING = createURI("ContextHashBasedString");

    /** Class nif:ContextOccurrence. */
    public static final URI CONTEXT_OCCURRENCE = createURI("ContextOccurrence");

    /** Class nif:LabelString. */
    public static final URI LABEL_STRING = createURI("LabelString");

    /** Class nif:NormalizedCollectionOccurrence. */
    public static final URI NORMALIZED_COLLECTION_OCCURRENCE = //
    createURI("NormalizedCollectionOccurrence");

    /** Class nif:NormalizedContextOccurrence. */
    public static final URI NORMALIZED_CONTEXT_OCCURRENCE = //
    createURI("NormalizedContextOccurrence");

    /** Class nif:OccurringString. */
    public static final URI OCCURRING_STRING = createURI("OccurringString");

    /** Class nif:OffsetBasedString. */
    public static final URI OFFSET_BASED_STRING = createURI("OffsetBasedString");

    /** Class nif:Paragraph. */
    public static final URI PARAGRAPH = createURI("Paragraph");

    /** Class nif:Phrase. */
    public static final URI PHRASE = createURI("Phrase");

    /** Class nif:RFC5147String. */
    public static final URI RFC5147_STRING = createURI("RFC5147String");

    /** Class nif:Sentence. */
    public static final URI SENTENCE = createURI("Sentence");

    /** Class nif:String. */
    public static final URI STRING = createURI("String");

    /** Class nif:Structure. */
    public static final URI STRUCTURE = createURI("Structure");

    /** Class nif:Title. */
    public static final URI TITLE = createURI("Title");

    /** Class nif:URIScheme. */
    public static final URI URISCHEME = createURI("URIScheme");

    /** Class nif:Word. */
    public static final URI WORD = createURI("Word");

    // PROPERTIES

    /** Property nif:after. */
    public static final URI AFTER = createURI("after");

    /** Property nif:anchorOf. */
    public static final URI ANCHOR_OF = createURI("anchorOf");

    /** Property nif:annotation. */
    public static final URI ANNOTATION = createURI("annotation");

    /** Property nif:before. */
    public static final URI BEFORE = createURI("before");

    /** Property nif:beginIndex. */
    public static final URI BEGIN_INDEX = createURI("beginIndex");

    /** Property nif:broaderContext. */
    public static final URI BROADER_CONTEXT = createURI("broaderContext");

    /** Property nif:class. */
    public static final URI CLASS = createURI("class");

    /** Property nif:classAnnotation. */
    public static final URI CLASS_ANNOTATION = createURI("classAnnotation");

    /** Property nif:endIndex. */
    public static final URI END_INDEX = createURI("endIndex");

    /** Property nif:firstWord. */
    public static final URI FIRST_WORD = createURI("firstWord");

    /** Property nif:head. */
    public static final URI HEAD = createURI("head");

    /** Property nif:inter. */
    public static final URI INTER = createURI("inter");

    /** Property nif:isString. */
    public static final URI IS_STRING = createURI("isString");

    /** Property nif:lastWord. */
    public static final URI LAST_WORD = createURI("lastWord");

    /** Property nif:lemma. */
    public static final URI LEMMA = createURI("lemma");

    /** Property nif:literalAnnotation. */
    public static final URI LITERAL_ANNOTATION = createURI("literalAnnotation");

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

    /** Property nif:occurrence. */
    public static final URI OCCURRENCE = createURI("occurrence");

    /** Property nif:oliaCategory. */
    public static final URI OLIA_CATEGORY = createURI("oliaCategory");

    /** Property nif:oliaCategoryConf. */
    public static final URI OLIA_CATEGORY_CONF = createURI("oliaCategoryConf");

    /** Property nif:oliaLink. */
    public static final URI OLIA_LINK = createURI("oliaLink");

    /** Property nif:oliaLinkConf. */
    public static final URI OLIA_LINK_CONF = createURI("oliaLinkConf");

    /** Property nif:opinion. */
    public static final URI OPINION = createURI("opinion");

    /** Property nif:posTag. */
    public static final URI POS_TAG = createURI("posTag");

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
    public static final URI SENTENCE_PROPERTY = createURI("sentence");

    /** Property nif:sentimentValue. */
    public static final URI SENTIMENT_VALUE = createURI("sentimentValue");

    /** Property nif:sourceUrl. */
    public static final URI SOURCE_URL = createURI("sourceUrl");

    /** Property nif:stem. */
    public static final URI STEM = createURI("stem");

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
    public static final URI WORD_PROPERTY = createURI("word");

    // HELPER METHODS

    private static URI createURI(final String localName) {
        return ValueFactoryImpl.getInstance().createURI(NAMESPACE, localName);
    }

    private NIF() {
    }

}
