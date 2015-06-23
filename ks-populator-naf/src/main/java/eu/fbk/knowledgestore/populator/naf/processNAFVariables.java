package eu.fbk.knowledgestore.populator.naf;

import java.io.File;
import java.io.Writer;
import java.util.Hashtable;
import java.util.LinkedList;

import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.DCTERMS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.populator.naf.model.NAF;
import eu.fbk.knowledgestore.populator.naf.model.NafHeader;
import eu.fbk.knowledgestore.populator.naf.model.Terms;
import eu.fbk.knowledgestore.populator.naf.model.Text;
import eu.fbk.knowledgestore.vocabulary.NWR;

public class processNAFVariables {

     NAF doc;
     NafHeader nafHeader;
     int mentionCounter = 0;
     DCTERMS dct = new DCTERMS();
     String nafPublicId;
     URI NAF_file_id;
     URI news_file_id;
     String PREFIX = "http://www.newsreader-project.eu/data/cars";
     Terms globalTerms;
     Text globalText;
     Hashtable<String, URI> nafLayerMapper = new Hashtable<String, URI>();
     Hashtable<String, URI> entityTypeMapper = new Hashtable<String, URI>();
     Hashtable<String, URI> timex3TypeMapper = new Hashtable<String, URI>();
     Hashtable<String, URI> valueTypeMapper = new Hashtable<String, URI>();
     Hashtable<String, URI> certaintyMapper = new Hashtable<String, URI>();
     Hashtable<String, URI> factualityMapper = new Hashtable<String, URI>();
     Hashtable<String, URI> polarityMapper = new Hashtable<String, URI>();
     Hashtable<String, URI> partOfSpeechMapper = new Hashtable<String, URI>();
     Hashtable<String, URI> eventClassMapper = new Hashtable<String, URI>();
     Hashtable<String, URI> entityClassMapper = new Hashtable<String, URI>();
     Hashtable<String, URI> timex3ModifierMapper = new Hashtable<String, URI>();
     Hashtable<String, URI> funtionInDocumentMapper = new Hashtable<String, URI>();
     Hashtable<String, URI> syntacticTypeMapper = new Hashtable<String, URI>();
     Hashtable<String, URI> tenseMapper = new Hashtable<String, URI>();
     Hashtable<String, URI> aspectMapper = new Hashtable<String, URI>();
     Hashtable<String, URI> tLinkTypeMapper = new Hashtable<String, URI>();
     Hashtable<String, URI> srlExternalRefResourceTypeMapper = new Hashtable<String, URI>();
     Hashtable<String, Record> mentionListHash = new Hashtable<String, Record>();
     Hashtable<String, Record> entityMentions = new Hashtable<String, Record>();
    Logger logger = LoggerFactory.getLogger(nafPopulator.class);
     Record newsFile2, nafFile2;
     Writer out;
     int entityMen = 0, corefMention = 0,corefMentionEvent = 0,corefMentionNotEvent = 0, timeMention = 0, srlMention = 0, entityMen2 = 0,
            corefMention2 = 0, timeMention2 = 0, srlMention2 = 0, rolewithEntity = 0,
            rolewithEntity2 = 0, rolewithoutEntity = 0, factualityMentions = 0,
            factualityMentions2 = 0, roleMentions = 0;
     int PER = 0, LOC = 0, ORG = 0, PRO = 0, fin = 0, mix = 0, no_mapping = 0;
     boolean logDebugActive = true, logErrorActive = true;
     String rawText = "";
     boolean storePartialInforInCaseOfError = false;
     File filePath = null;
       processNAFVariables() {
     	PER = 0;
         LOC = 0;
         ORG = 0;
         PRO = 0;
         fin = 0;
         mix = 0;
         no_mapping = 0;
         entityMen2 = 0;
         corefMention2 = 0;
         timeMention2 = 0;
         srlMention2 = 0;
         rolewithEntity2 = 0;
         factualityMentions2 = 0;
         entityMen = 0;
         corefMention = 0;
         corefMentionEvent = 0;
         corefMentionNotEvent = 0;
         timeMention = 0;
         srlMention = 0;
         rolewithEntity = 0;
         rolewithoutEntity = 0;
         factualityMentions = 0;
         roleMentions = 0;
         rawText = "";
         nafLayerMapper = new Hashtable<String, URI>();
         entityTypeMapper = new Hashtable<String, URI>();
         timex3TypeMapper = new Hashtable<String, URI>();
         valueTypeMapper = new Hashtable<String, URI>();
         certaintyMapper = new Hashtable<String, URI>();
         factualityMapper = new Hashtable<String, URI>();
         polarityMapper = new Hashtable<String, URI>();
         partOfSpeechMapper = new Hashtable<String, URI>();
         eventClassMapper = new Hashtable<String, URI>();
         entityClassMapper = new Hashtable<String, URI>();
         timex3ModifierMapper = new Hashtable<String, URI>();
         funtionInDocumentMapper = new Hashtable<String, URI>();
         syntacticTypeMapper = new Hashtable<String, URI>();
         tenseMapper = new Hashtable<String, URI>();
         aspectMapper = new Hashtable<String, URI>();
         tLinkTypeMapper = new Hashtable<String, URI>();
         srlExternalRefResourceTypeMapper = new Hashtable<String, URI>();
         mentionListHash = new Hashtable<String, Record>();
         entityMentions = new Hashtable<String, Record>();
         valueTypeMapper.put("", NWR.VALUE_PERCENT);
         valueTypeMapper.put("", NWR.VALUE_MONEY);
         valueTypeMapper.put("", NWR.VALUE_QUANTITY);

         certaintyMapper.put("", NWR.CERTAIN);
         certaintyMapper.put("", NWR.UNCERTAIN);

         factualityMapper.put("", NWR.FACTUAL);
         factualityMapper.put("", NWR.COUNTERFACTUAL);
         factualityMapper.put("", NWR.NON_FACTUAL);

         polarityMapper.put("", NWR.POLARITY_POS);
         polarityMapper.put("", NWR.POLARITY_NEG);

         partOfSpeechMapper.put("N", NWR.POS_NOUN);
         partOfSpeechMapper.put("V", NWR.POS_VERB);
         partOfSpeechMapper.put("", NWR.POS_OTHER);

         eventClassMapper.put("cognition", NWR.EVENT_SPEECH_COGNITIVE);
         eventClassMapper.put("cognitive", NWR.EVENT_SPEECH_COGNITIVE);
         eventClassMapper.put("communication", NWR.EVENT_SPEECH_COGNITIVE);
         eventClassMapper.put("grammatical", NWR.EVENT_GRAMMATICAL);
         eventClassMapper.put("contextual", NWR.EVENT_OTHER);

         timex3TypeMapper.put("DATE", NWR.TIMEX3_DATE);
         timex3TypeMapper.put("TIME", NWR.TIMEX3_TIME);
         timex3TypeMapper.put("DURATION", NWR.TIMEX3_DURATION);
         timex3TypeMapper.put("SET", NWR.TIMEX3_SET);

         entityClassMapper.put("", NWR.ENTITY_CLASS_SPC);
         entityClassMapper.put("", NWR.ENTITY_CLASS_GEN);
         entityClassMapper.put("", NWR.ENTITY_CLASS_USP);
         entityClassMapper.put("", NWR.ENTITY_CLASS_NEG);

         timex3ModifierMapper.put("", NWR.MOD_BEFORE);
         timex3ModifierMapper.put("", NWR.MOD_ON_OR_BEFORE);
         timex3ModifierMapper.put("", NWR.MOD_MID);
         timex3ModifierMapper.put("", NWR.MOD_END);
         timex3ModifierMapper.put("", NWR.MOD_AFTER);
         timex3ModifierMapper.put("", NWR.MOD_ON_OR_AFTER);
         timex3ModifierMapper.put("", NWR.MOD_LESS_THAN);
         timex3ModifierMapper.put("", NWR.MOD_MORE_THAN);
         timex3ModifierMapper.put("", NWR.MOD_EQUAL_OR_LESS);
         timex3ModifierMapper.put("", NWR.MOD_EQUAL_OR_MORE);
         timex3ModifierMapper.put("", NWR.MOD_START);
         timex3ModifierMapper.put("", NWR.MOD_APPROX);

         funtionInDocumentMapper.put("", NWR.FUNCTION_CREATION_TIME);
         funtionInDocumentMapper.put("", NWR.FUNCTION_EXPIRATION_TIME);
         funtionInDocumentMapper.put("", NWR.FUNCTION_MODIFICATION_TIME);
         funtionInDocumentMapper.put("", NWR.FUNCTION_PUBLICATION_TIME);
         funtionInDocumentMapper.put("", NWR.FUNCTION_RELEASE_TIME);
         funtionInDocumentMapper.put("", NWR.FUNCTION_RECEPTION_TIME);
         funtionInDocumentMapper.put("", NWR.FUNCTION_NONE);

         syntacticTypeMapper.put("", NWR.SYNTACTIC_TYPE_NAM);
         syntacticTypeMapper.put("", NWR.SYNTACTIC_TYPE_NOM);
         syntacticTypeMapper.put("", NWR.SYNTACTIC_TYPE_PRO);
         syntacticTypeMapper.put("", NWR.SYNTACTIC_TYPE_PTV);
         syntacticTypeMapper.put("", NWR.SYNTACTIC_TYPE_PRE);
         syntacticTypeMapper.put("", NWR.SYNTACTIC_TYPE_HLS);
         syntacticTypeMapper.put("", NWR.SYNTACTIC_TYPE_CONJ);
         syntacticTypeMapper.put("", NWR.SYNTACTIC_TYPE_APP);
         syntacticTypeMapper.put("", NWR.SYNTACTIC_TYPE_ARC);

         entityTypeMapper.put("per", NWR.ENTITY_TYPE_PER);
         entityTypeMapper.put("loc", NWR.ENTITY_TYPE_LOC);
         entityTypeMapper.put("org", NWR.ENTITY_TYPE_ORG);
         entityTypeMapper.put("art", NWR.ENTITY_TYPE_PRO);
         entityTypeMapper.put("pro", NWR.ENTITY_TYPE_PRO);
         entityTypeMapper.put("fin", NWR.ENTITY_TYPE_FIN);
         entityTypeMapper.put("mix", NWR.ENTITY_TYPE_MIX);

         tenseMapper.put("", NWR.TENSE_FUTURE);
         tenseMapper.put("", NWR.TENSE_PAST);
         tenseMapper.put("", NWR.TENSE_PRESENT);
         tenseMapper.put("", NWR.TENSE_INFINITIVE);
         tenseMapper.put("", NWR.TENSE_PRESPART);
         tenseMapper.put("", NWR.TENSE_PASTPART);
         tenseMapper.put("", NWR.TENSE_NONE);

         aspectMapper.put("", NWR.ASPECT_PROGRESSIVE);
         aspectMapper.put("", NWR.ASPECT_PERFECTIVE);
         aspectMapper.put("", NWR.ASPECT_PERFECTIVE_PROGRESSIVE);
         aspectMapper.put("", NWR.ASPECT_NONE);

         nafLayerMapper.put("raw", NWR.LAYER_RAW);
         nafLayerMapper.put("text", NWR.LAYER_TEXT);
         nafLayerMapper.put("terms", NWR.LAYER_TERMS);
         nafLayerMapper.put("deps", NWR.LAYER_DEPS);
         nafLayerMapper.put("chunks", NWR.LAYER_CHUNKS);
         nafLayerMapper.put("entities", NWR.LAYER_ENTITIES);
         nafLayerMapper.put("coreferences", NWR.LAYER_COREFERENCES);
         nafLayerMapper.put("srl", NWR.LAYER_SRL);
         nafLayerMapper.put("constituency", NWR.LAYER_CONSTITUENCY);
         nafLayerMapper.put("timeExpressions", NWR.LAYER_TIME_EXPRESSIONS);
         nafLayerMapper.put("factuality", NWR.LAYER_FACTUALITY);

         nafLayerMapper.put("opinions",  NWR.LAYER_OPINIONS); 
         nafLayerMapper.put("temporalRelations",  NWR.LAYER_TEMPORAL_RELATIONS); 
         nafLayerMapper.put("causalRelations",  NWR.LAYER_CAUSAL_RELATIONS); 
         nafLayerMapper.put("vua-multiword-tagger",  NWR.LAYER_VUA_MULTIWORD_TAGGER); 
         nafLayerMapper.put("vua-event-coref-intradoc-lemma-baseline",  NWR.LAYER_VUA_EVENT_COREF_INTRADOC_LEMMA_BASELINE);

 	// patch for wrong value layer="coreference"
         nafLayerMapper.put("coreference", NWR.LAYER_COREFERENCES);

 	// patch for wrong value layer="time_expressions" or layer="timex3"
         nafLayerMapper.put("time_expressions", NWR.LAYER_TIME_EXPRESSIONS);
         nafLayerMapper.put("timex3", NWR.LAYER_TIME_EXPRESSIONS);


         tLinkTypeMapper.put("", NWR.TLINK_BEFORE);
         tLinkTypeMapper.put("", NWR.TLINK_AFTER);
         tLinkTypeMapper.put("", NWR.TLINK_INCLUDES);
         tLinkTypeMapper.put("", NWR.TLINK_MEASURE);
         tLinkTypeMapper.put("", NWR.TLINK_IS_INCLUDED);
         tLinkTypeMapper.put("", NWR.TLINK_SIMULTANEOUS);
         tLinkTypeMapper.put("", NWR.TLINK_IAFTER);
         tLinkTypeMapper.put("", NWR.TLINK_IBEFORE);
         tLinkTypeMapper.put("", NWR.TLINK_BEGINS);
         tLinkTypeMapper.put("", NWR.TLINK_ENDS);
         tLinkTypeMapper.put("", NWR.TLINK_BEGUN_BY);
         tLinkTypeMapper.put("", NWR.TLINK_ENDED_BY);

         srlExternalRefResourceTypeMapper.put("PropBank", NWR.PROPBANK_REF);
         srlExternalRefResourceTypeMapper.put("VerbNet", NWR.VERBNET_REF);
         srlExternalRefResourceTypeMapper.put("FrameNet", NWR.FRAMENET_REF);
         srlExternalRefResourceTypeMapper.put("NomBank", NWR.NOMBANK_REF);
         srlExternalRefResourceTypeMapper.put("ESO", NWR.ESO_REF);
     }
}
