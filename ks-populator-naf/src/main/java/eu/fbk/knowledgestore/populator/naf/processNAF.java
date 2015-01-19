package eu.fbk.knowledgestore.populator.naf;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.DCTERMS;
import org.openrdf.model.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.populator.naf.model.Coref;
import eu.fbk.knowledgestore.populator.naf.model.Coreferences;
import eu.fbk.knowledgestore.populator.naf.model.Entities;
import eu.fbk.knowledgestore.populator.naf.model.Entity;
import eu.fbk.knowledgestore.populator.naf.model.ExternalRef;
import eu.fbk.knowledgestore.populator.naf.model.ExternalReferences;
import eu.fbk.knowledgestore.populator.naf.model.Factualitylayer;
import eu.fbk.knowledgestore.populator.naf.model.Factvalue;
import eu.fbk.knowledgestore.populator.naf.model.FileDesc;
import eu.fbk.knowledgestore.populator.naf.model.LinguisticProcessors;
import eu.fbk.knowledgestore.populator.naf.model.Lp;
import eu.fbk.knowledgestore.populator.naf.model.NAF;
import eu.fbk.knowledgestore.populator.naf.model.NafHeader;
import eu.fbk.knowledgestore.populator.naf.model.Predicate;
import eu.fbk.knowledgestore.populator.naf.model.Public;
import eu.fbk.knowledgestore.populator.naf.model.Raw;
import eu.fbk.knowledgestore.populator.naf.model.References;
import eu.fbk.knowledgestore.populator.naf.model.Role;
import eu.fbk.knowledgestore.populator.naf.model.Span;
import eu.fbk.knowledgestore.populator.naf.model.Srl;
import eu.fbk.knowledgestore.populator.naf.model.Target;
import eu.fbk.knowledgestore.populator.naf.model.Term;
import eu.fbk.knowledgestore.populator.naf.model.Terms;
import eu.fbk.knowledgestore.populator.naf.model.Text;
import eu.fbk.knowledgestore.populator.naf.model.TimeExpressions;
import eu.fbk.knowledgestore.populator.naf.model.Timex3;
import eu.fbk.knowledgestore.populator.naf.model.Wf;
import eu.fbk.knowledgestore.vocabulary.KS;
import eu.fbk.knowledgestore.vocabulary.NIF;
import eu.fbk.knowledgestore.vocabulary.NWR;

public class processNAF {

    static NAF doc;
    static NafHeader nafHeader;
    static int mentionCounter = 0;
    static DCTERMS dct = new DCTERMS();
    static String nafPublicId;
    static URI NAF_file_id;
    static URI news_file_id;
    static String PREFIX = "http://www.newsreader-project.eu/data/cars";
    static Terms globalTerms;
    static Text globalText;
    static Hashtable<String, URI> nafLayerMapper = new Hashtable<String, URI>();
    static Hashtable<String, URI> entityTypeMapper = new Hashtable<String, URI>();
    static Hashtable<String, URI> timex3TypeMapper = new Hashtable<String, URI>();
    static Hashtable<String, URI> valueTypeMapper = new Hashtable<String, URI>();
    static Hashtable<String, URI> certaintyMapper = new Hashtable<String, URI>();
    static Hashtable<String, URI> factualityMapper = new Hashtable<String, URI>();
    static Hashtable<String, URI> polarityMapper = new Hashtable<String, URI>();
    static Hashtable<String, URI> partOfSpeechMapper = new Hashtable<String, URI>();
    static Hashtable<String, URI> eventClassMapper = new Hashtable<String, URI>();
    static Hashtable<String, URI> entityClassMapper = new Hashtable<String, URI>();
    static Hashtable<String, URI> timex3ModifierMapper = new Hashtable<String, URI>();
    static Hashtable<String, URI> funtionInDocumentMapper = new Hashtable<String, URI>();
    static Hashtable<String, URI> syntacticTypeMapper = new Hashtable<String, URI>();
    static Hashtable<String, URI> tenseMapper = new Hashtable<String, URI>();
    static Hashtable<String, URI> aspectMapper = new Hashtable<String, URI>();
    static Hashtable<String, URI> tLinkTypeMapper = new Hashtable<String, URI>();
    static Hashtable<String, URI> srlExternalRefResourceTypeMapper = new Hashtable<String, URI>();
    static Hashtable<String, Record> mentionListHash = new Hashtable<String, Record>();
    static LinkedList<Record> entityMentions = new LinkedList<Record>();
    private static Logger logger = LoggerFactory.getLogger(nafPopulator.class);
    static Record newsFile2, nafFile2;
    static Writer out;
    static int entityMen = 0, corefMention = 0,corefMentionEvent = 0,corefMentionNotEvent = 0, timeMention = 0, srlMention = 0, entityMen2 = 0,
            corefMention2 = 0, timeMention2 = 0, srlMention2 = 0, rolewithEntity = 0,
            rolewithEntity2 = 0, rolewithoutEntity = 0, factualityMentions = 0,
            factualityMentions2 = 0, roleMentions = 0;
    static int PER = 0, LOC = 0, ORG = 0, PRO = 0, fin = 0, mix = 0, no_mapping = 0;
    static boolean logDebugActive = true, logErrorActive = true;
    static String rawText = "";
    static boolean storePartialInforInCaseOfError = false;
    static File filePath = null;

    public static void main(String[] args) throws JAXBException, IOException {
        // args[0] is the path, [args[1] is the disabled_Items]
        String disabled_Items = "", path = "";
        if (args.length > 0) {
            path = args[0];
            if (args.length > 1)
                disabled_Items = args[1];
        } else {
            System.err
                    .println("eu.fbk.knowledgestore.populator.naf.processNAF path disabled_items \ndisabled_items = [Entities|Mentions|Resources] ");
            System.exit(-1);
        }
        analyzePathAndRunSystem(path, disabled_Items);
    }

    public static KSPresentation init(String fPath, Writer inout, String disabled_Items,
            boolean store_partical_info) throws JAXBException, IOException {
        storePartialInforInCaseOfError = store_partical_info;
        out = inout;
        statistics stat = readFile(fPath, disabled_Items);
        KSPresentation returned = new KSPresentation();
        returned.setNaf_file_path(fPath);
        returned.setNews(rawText);
        returned.setMentions(mentionListHash);
        returned.setNaf(nafFile2);
        returned.setNewsResource(newsFile2);
        returned.setStats(stat);
        clearObjects();
        return returned;
    }

    private static void clearObjects() {
        doc = null;
        nafHeader = null;
        nafPublicId = null;
        NAF_file_id = null;
        news_file_id = null;
        globalTerms = null;
        globalText = null;
        nafLayerMapper = null;
        entityTypeMapper = null;
        timex3TypeMapper = null;
        valueTypeMapper = null;
        certaintyMapper = null;
        factualityMapper = null;
        polarityMapper = null;
        partOfSpeechMapper = null;
        eventClassMapper = null;
        entityClassMapper = null;
        timex3ModifierMapper = null;
        funtionInDocumentMapper = null;
        syntacticTypeMapper = null;
        tenseMapper = null;
        aspectMapper = null;
        tLinkTypeMapper = null;
        srlExternalRefResourceTypeMapper = null;
        mentionListHash = null;
        entityMentions = null;
        rawText = null;
    }

    private static void analyzePathAndRunSystem(String path, String disabled_Items)
            throws JAXBException, IOException {
        filePath = new File(path);
        if (filePath.exists() && filePath.isDirectory()) {
            // create report file in the same directory of running the system
            out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(
                    filePath.getPath(), "report.txt")), "utf-8"));
            File[] listOfFiles = filePath.listFiles();
            for (int i = 0; i < listOfFiles.length; i++) {
                if (listOfFiles[i].isFile() && listOfFiles[i].getName().endsWith(".naf")) {
                    System.err.println(i + "=" + listOfFiles[i].getName());
                    out.append("\n" + i + "=" + listOfFiles[i].getName() + "\n");
                    readFile(listOfFiles[i].getPath(), disabled_Items);
                }
                out.flush();
                clearObjects();
                System.gc();
                Runtime.getRuntime().gc();
            }
        } else if (filePath.exists() && filePath.isFile()) {
            // create report file in the same directory of running the system
            out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(
                    filePath.getPath() + ".report.txt")), "utf-8"));
            out.append(filePath.getPath() + "\n");
            readFile(filePath.getPath(), disabled_Items);
        }
        out.flush();
        out.close();
    }

    public static statistics readFile(String filepath, String disabled_Items) throws JAXBException,
            IOException {
        storePartialInforInCaseOfError = true;
	filePath = new File(filepath);
        logDebug("Start working with (" + filePath.getName() + ")");
        String disabledItems = "";// Default empty so generate all layers of data
        if (disabled_Items != null
                && (disabled_Items.matches("(?i)Entity") || disabled_Items.contains("(?i)Mention") || disabled_Items.contains("(?i)Resource"))) {
            disabledItems = disabled_Items;
            logDebug("Disable layer: " + disabledItems);
        }
        init();
        readNAFFile(filePath);
        for (Object obj : doc
                .getNafHeaderOrRawOrTextOrTermsOrDepsOrChunksOrEntitiesOrCoreferencesOrConstituencyOrTimeExpressionsOrFactualitylayerOrTunitsOrLocationsOrDates()) {
            if (obj instanceof NafHeader) {
                getNAFHEADERMentions((NafHeader) obj);
            } else if (obj instanceof Raw) {
                // raw text
                rawText = ((Raw) obj).getvalue();
            } else if (obj instanceof Text) {
                globalText = (Text) obj;
            } else if (obj instanceof Entities) {
		getEntitiesMentions((Entities) obj, disabledItems);
            } 
	    else if (obj instanceof Coreferences) {
                getCoreferencesMentions((Coreferences) obj);
            } 
	    else if (obj instanceof TimeExpressions) {
                getTimeExpressionsMentions((TimeExpressions) obj);
            } 
	    else if (obj instanceof Factualitylayer) {
                getFactualityMentions((Factualitylayer) obj);
            }
	    else if (obj instanceof Terms) {
                globalTerms = (Terms) obj;
            } else {
                // logError("Error:Uncatchable Object:"+obj);
            }
        }
	getSRLMentions();

	// logDebug("ROL1 before dumpStack()") ; Thread.currentThread().dumpStack();

	fixMentions();

        logDebug("End of NAF populating.");
        statistics st = new statistics();
        st.setObjectMention((corefMention2+entityMen2));
        st.setPER(PER);
        st.setORG(ORG);
        st.setLOC(LOC);
        st.setFin(fin);
        st.setMix(mix);
        st.setPRO(PRO);
        st.setNo_mapping(no_mapping);
        st.setTimeMention(timeMention2);
        st.setEventMention((factualityMentions2 + srlMention2));
        st.setParticipationMention(rolewithEntity2);
        st.setEntity(entityMen);
        st.setCoref(corefMention);
        st.setCorefMentionEvent(corefMentionEvent);
        st.setCorefMentionNotEvent(corefMentionNotEvent);
        st.setFactuality(factualityMentions);
        st.setRole(roleMentions);
        st.setRolewithEntity(rolewithEntity);
        st.setRolewithoutEntity(rolewithoutEntity);
        st.setSrl(srlMention);
        st.setTimex(timeMention);
        
        logDebug(st.getStats());
        return st;
    }

    private static void getEntitiesMentions(Entities obj, String disabledItems) {
        if (!checkHeaderTextTerms()) {
            logError("Error: populating stopped");
        } else {
            logDebug("Start mapping the Entities mentions:");
        }
        for (Entity entObj : ((Entities) obj).getEntity()) {
            String deg = "";
            int referencesElements = 0;
            String charS = null;
	    /* 
	       process <references> or <externalReferences>
	    */
            for (Object generalEntObj : entObj.getReferencesOrExternalReferences()) {
                if (generalEntObj instanceof References) {
		    /*
		      process <references>
		    */
                    referencesElements++;
                    if (((References) generalEntObj).getSpan().size() < 1) {
                        logWarn("Every entity must contain a 'span' element inside 'references'");
                    }
                    if (((References) generalEntObj).getSpan().size() > 1) {
                        logWarn("xpath(///NAF/entities/entity/references/span/), spanSize("
                                + ((References) generalEntObj).getSpan().size()
                                + ") Every entity must contain a unique 'span' element inside 'references'");
                    }
                    for (Span spansObj : ((References) generalEntObj).getSpan()) {
			boolean addMentionFlag = true;

                        if (spansObj.getTarget().size() < 1) {
			    addMentionFlag = false;
                            logWarn("Every span in an entity must contain at least one target inside");
			    continue;
                        }
                       
                        Record m = Record.create();
                        deg += "RDF.TYPE:OBJECT_MENTION,ENTITY_MENTION,MENTION";
                        m.add(RDF.TYPE, NWR.OBJECT_MENTION, NWR.ENTITY_MENTION, KS.MENTION);
                        deg = "MENTION_OF:" + news_file_id.stringValue() + "|" + deg;
                        m.add(KS.MENTION_OF, news_file_id);

                        if (((References) generalEntObj).getSpan().size() > 1) {
                            m.add(NWR.LOCAL_COREF_ID, entObj.getId());
                            deg += "|LOCAL_COREF_ID:" + entObj.getId();
                        }
                        generateTheMIdAndSetID(spansObj, m);
                        charS = m.getUnique(NIF.BEGIN_INDEX, Integer.class) + "," + m.getUnique(NIF.END_INDEX, Integer.class);
                        deg = "MentionId:" + m.getID() + "|" + deg;


			/*
			  don't use predefined types from guidelines but keep the mention with type provided in the NAF
			*/
			boolean keepEntityTypeProvidedByNaf = true;
			String type3charLC = entObj.getType().substring(0, 3).toLowerCase();

			if (keepEntityTypeProvidedByNaf) {
			    URI dynamicTypeUri = ValueFactoryImpl.getInstance().createURI(NWR.NAMESPACE, "entity_type_" + entObj.getType().toLowerCase());
			    m.add(NWR.ENTITY_TYPE, dynamicTypeUri);
			    deg += "|ENTITY_TYPE:" + dynamicTypeUri;
			    logDebug("ROL1: <entity> added new mention for id " + entObj.getId() + ", charSpan |" 
				    + getCharSpanFromSpan(spansObj) + "|, type " + dynamicTypeUri);
			} else {
			    if (entityTypeMapper.containsKey(type3charLC)
				&& entityTypeMapper.get(type3charLC) != null) {
				m.add(NWR.ENTITY_TYPE, entityTypeMapper.get(type3charLC));
				deg += "|ENTITY_TYPE:" + entityTypeMapper.get(type3charLC);
				logDebug("ROL1: <entity> STRANGE added new mention for id " + entObj.getId() + ", charSpan |" 
					+ getCharSpanFromSpan(spansObj) + "|, type " + entityTypeMapper.get(type3charLC));
			    } else {
				addMentionFlag = false;
				logWarn("xpath(//NAF/entities/entity/@type),type(" + entObj.getType() + "), id(" 
					+ entObj.getId() + ") NO mapping for it");
				no_mapping++;
			    }
			}


			if (addMentionFlag) {
			    if (addOrMergeAMention(m)==1) {
				entityMentions.addLast(m);
				if (type3charLC.equalsIgnoreCase("PER"))
				    PER++;
				if (type3charLC.equalsIgnoreCase("LOC"))
				    LOC++;
				if (type3charLC.equalsIgnoreCase("ORG"))
				    ORG++;
				if (type3charLC.equalsIgnoreCase("PRO"))
				    PRO++;
				if (type3charLC.equalsIgnoreCase("fin"))
				    fin++;
				if (type3charLC.equalsIgnoreCase("mix"))
				    mix++;
				entityMen2++;
				entityMen++;
			    }
			}
                    }
                } else if (generalEntObj instanceof ExternalReferences) {
		    /* 
		       process <externalReferences>
		    */

		    // do it only if management of (KS) "Entity" layer is not disabled
		    //
		    if (! disabledItems.matches("(?i)Entity")) {
			boolean firstTimeFlag = true;
			String chosenReferenceValue = null;
			for (ExternalRef exRObj : ((ExternalReferences) generalEntObj) .getExternalRef()) {
			    if (referencesElements < 1) {
				logWarn("Every entity must contain a 'references' element:not possible to add ExternalRef to null.");
				continue;
			    }
			    String resourceValue = exRObj.getResource();
			    String referenceValue = exRObj.getReference();

			    // choose as referenceValue the one provided by 'vua-type-reranker' if present, otherwise take the first value
			    if (firstTimeFlag) {
				chosenReferenceValue = new String(referenceValue);
				firstTimeFlag = false;
			    } else if (resourceValue.matches("(?i).*type-reranker.*")) {
				chosenReferenceValue = new String(referenceValue);
			    }
			}

			URIImpl chosenReferenceURI = new URIImpl(chosenReferenceValue);
			if (charS != null)
			    mentionListHash.get(charS).add(KS.REFERS_TO, chosenReferenceURI);
			deg += "|REFERS_TO:" + chosenReferenceValue;
			entityMentions.getLast().add(KS.REFERS_TO, chosenReferenceURI);
                    }
                } // end of   if (generalEntObj instanceof ExternalReferences) {

            } // end of    for (Object generalEntObj  : entObj.getReferencesOrExternalReferences())

            logDebug(deg);
            if (referencesElements < 1) {
                logWarn("Every entity must contain a 'references' element");
            }
        }
    }

    private static void getTimeExpressionsMentions(TimeExpressions obj) {
        if (!checkHeaderTextTerms()) {
            logError("Error: populating interrupted");
        } else {
            logDebug("Start mapping the TimeExpressions mentions:");
        }
        for (Timex3 tmxObj : ((TimeExpressions) obj).getTimex3()) {

	    Span tmxSpan = tmxObj.getSpan();
	    String tmxTypeUC = tmxObj.getType().toUpperCase();
	    boolean keepTimeTypeProvidedByNaf = true;

	    // patch for timex3 without <span>
	    if ((tmxSpan == null) || (tmxSpan.getTarget().size() < 1)) {
		logWarn("skipping timex3 without span, id is " + tmxObj.getId());
		continue;
	    }

            String deg = "";
            Record m = Record.create();
            m.add(RDF.TYPE, NWR.TIME_MENTION, NWR.TIME_OR_EVENT_MENTION, NWR.ENTITY_MENTION,
                    KS.MENTION);
            deg += "|TYPE:TIME_MENTION,TIME_OR_EVENT_MENTION,ENTITY_MENTION,MENTION";
	    m.add(KS.MENTION_OF, news_file_id);
            LinkedList<Wf> wordsL = fromSpanGetAllMentionsTmx(((Span) tmxSpan).getTarget());
            generateMIDAndSetIdWF(wordsL, m);
            deg = "MentionId:" + m.getID() + deg;

	    if (keepTimeTypeProvidedByNaf) {
		URI dynamicTypeUri = ValueFactoryImpl.getInstance().createURI(NWR.NAMESPACE, "timex3_" + tmxTypeUC.toLowerCase());
                m.add(NWR.TIME_TYPE, dynamicTypeUri);
                deg += "|TIME_TYPE:" + dynamicTypeUri;
		logDebug("ROL1: <timex3> added new mention for id " + tmxObj.getId() + ", type " + dynamicTypeUri);
	    } else {
		if (timex3TypeMapper.containsKey(tmxTypeUC)
                    && timex3TypeMapper.get(tmxTypeUC) != null) {
		    m.add(NWR.TIME_TYPE, timex3TypeMapper.get(tmxTypeUC));
		    deg += "|TIME_TYPE:" + timex3TypeMapper.get(tmxTypeUC);
		    logDebug("ROL1: <timex3> STRANGE added new mention for id " + tmxObj.getId() 
			    + ", type " + timex3TypeMapper.get(tmxTypeUC));
		} else {
		    logWarn("xpath(//NAF/timeExpressions/timex3/@type), type(" + tmxTypeUC
			    + "), No mapping.");
		}
	    }
            if (tmxObj.getBeginPoint() != null) {
                m.add(NWR.BEGIN_POINT, tmxObj.getBeginPoint());
                deg += "|BEGIN_POINT:" + tmxObj.getBeginPoint();
            }
	    if (tmxObj.getEndPoint() != null) {
                m.add(NWR.END_POINT, tmxObj.getEndPoint());
                deg += "|END_POINT:" + tmxObj.getEndPoint();
            }
            if (tmxObj.getQuant() != null) {
                m.add(NWR.QUANT, tmxObj.getQuant());
                deg += "|QUANT:" + tmxObj.getQuant();
            }
            if (tmxObj.getFreq() != null) {
                m.add(NWR.FREQ, tmxObj.getFreq());
                deg += "|FREQ:" + tmxObj.getFreq();
            }
            if (tmxObj.getFunctionInDocument() != null) {
                m.add(NWR.FUNCTION_IN_DOCUMENT, tmxObj.getFunctionInDocument());
                deg += "|FUNCTION_IN_DOCUMENT:" + tmxObj.getFunctionInDocument();
            }
            if (tmxObj.getTemporalFunction() != null) {
                m.add(NWR.TEMPORAL_FUNCTION, tmxObj.getTemporalFunction());
                deg += "|TEMPORAL_FUNCTION:" + tmxObj.getTemporalFunction();
            }
            if (tmxObj.getValue() != null) {
                m.add(NWR.VALUE, tmxObj.getValue());
                deg += "|VALUE:" + tmxObj.getValue();
            }
            if (tmxObj.getValueFromFunction() != null) {
                m.add(NWR.VALUE_FROM_FUNCTION, tmxObj.getValueFromFunction());
                deg += "|VALUE_FROM_FUNCTION:" + tmxObj.getValueFromFunction();
            }
            if (tmxObj.getMod() != null) {
                m.add(NWR.MOD, tmxObj.getMod());
                deg += "|MOD:" + tmxObj.getMod();
            }
            if (tmxObj.getAnchorTimeID() != null) {
                m.add(NWR.ANCHOR_TIME, tmxObj.getAnchorTimeID());
                deg += "|ANCHOR_TIME:" + tmxObj.getAnchorTimeID();
            }
            logDebug(deg);
            int addedNew = addOrMergeAMention(m);
            if (addedNew==1){
                timeMention2++;
                timeMention++;
            }
            entityMentions.addLast(m);
        }
    }

    private static void getFactualityMentions(Factualitylayer obj) {
        if (!checkHeaderTextTerms()) {
            logError("Error: populating interrupted");
        } else {
            logDebug("Start mapping the Factuality mentions:");
        }
        for (Factvalue fvObj : ((Factualitylayer) obj).getFactvalue()) {
            String deg = "";
            Record m = Record.create();
            m.add(RDF.TYPE, NWR.EVENT_MENTION, NWR.TIME_OR_EVENT_MENTION, NWR.ENTITY_MENTION,
                    KS.MENTION);
            deg += "|TYPE:EVENT_MENTION,TIME_OR_EVENT_MENTION,ENTITY_MENTION,MENTION";
	    m.add(KS.MENTION_OF, news_file_id);
            LinkedList<Target> tarlist = new LinkedList<Target>();
            Target tmp = new Target();
            tmp.setId(fvObj.getId());
            tarlist.addLast(tmp);
            LinkedList<Wf> wordsL = fromSpanGetAllMentionsTmx(tarlist);
            generateMIDAndSetIdWF(wordsL, m);
            deg = "MentionId:" + m.getID() + deg;
            // fvObj.getPrediction(); //TODO we need to verify the mapping here
            // m.add(NWR.CERTAINTY, fvObj.getPrediction());
            // m.add(NWR.FACTUALITY, fvObj.getPrediction());
            if (fvObj.getConfidence() != null) {
                m.add(NWR.FACTUALITY_CONFIDENCE, fvObj.getConfidence());
                deg += "|FACTUALITY_CONFIDENCE:" + fvObj.getConfidence();
            }
            logDebug(deg);
            int addedNew = addOrMergeAMention(m);
            if (addedNew==1){
                factualityMentions2++;
                factualityMentions++;
            }
            entityMentions.addLast(m);

        }

    }

    private static URIImpl getUriForSrlExternalRefResource(String type, String value) {
	String prefix = null;
	if (type.equalsIgnoreCase("PropBank")) {
	    prefix = "http://www.newsreader-project.eu/propbank/";
	} else if (type.equalsIgnoreCase("VerbNet")) {
	    prefix = "http://www.newsreader-project.eu/verbnet/";
	} else if (type.equalsIgnoreCase("FrameNet")) {
	    prefix = "http://www.newsreader-project.eu/framenet/";
	} else if (type.equalsIgnoreCase("NomBank")) {
	    prefix = "http://www.newsreader-project.eu/nombank/";
	} else if (type.equalsIgnoreCase("ESO")) {
	    prefix = "http://www.newsreader-project.eu/domain-ontology#";
	}
	if (prefix != null) {
	    return new URIImpl(prefix + value);
	} else {
	    return null;
	}
    }

    private static void getSRLMentions() {
        Srl obj = doc.getSrl();
        if (!checkHeaderTextTerms()) {
            logError("Error: populating interrupted");
        } else {
            logDebug("Start mapping the Srl mentions:");
        }

	if ((obj == null) || (((Srl) obj).getPredicate() == null)) {
            logError("skipped missing xpath(//NAF/srl)");
	    return;
	}
	/* 
	   Iterate over the <predicate>s
	*/
	for (Predicate prdObj : ((Srl) obj).getPredicate()) {
            String deg = "";
            Record mtmp = null;
            String predicateID = prdObj.getId();
            boolean firstSpanFound = false;
            int predicatExtRef = 0;
            String eventMentionId = null;
	    String predicateCharSpan = null;
            LinkedList<Term> eventTermList = new LinkedList<Term>();

	    /* 
	       create an EVENT_MENTION for the <predicate>
	    */
            if (prdObj.getSpan() instanceof Span) {
                if (firstSpanFound) {
                    logWarn("Srl should have one span only! ");
                }
                if (!firstSpanFound) {
                    firstSpanFound = true;
                }
		predicateCharSpan = getCharSpanFromSpan(prdObj.getSpan());
                mtmp = Record.create();
                mtmp.add(RDF.TYPE, NWR.EVENT_MENTION, NWR.TIME_OR_EVENT_MENTION,
                        NWR.ENTITY_MENTION, KS.MENTION);
                deg = "|TYPE:EVENT_MENTION,TIME_OR_EVENT_MENTION,ENTITY_MENTION,MENTION";
		mtmp.add(KS.MENTION_OF, news_file_id);
                for (Target tars : prdObj.getSpan().getTarget()) {
                    tars.getId();
                    Term eventTerm = getTermfromTermId((Term) tars.getId());
                    eventTermList.addLast(eventTerm);
                    if (eventTerm.getLemma() != null) {
                        mtmp.add(NWR.PRED, eventTerm.getLemma());
                        deg += "|PRED:" + eventTerm.getLemma();
                    }
                    if (eventTerm.getPos() != null) {
			URI posVal = (eventTerm.getPos().equals("V") || 
				      eventTerm.getPos().equals("N")) 
			    ? partOfSpeechMapper.get(eventTerm.getPos())
			    : partOfSpeechMapper.get("");
                        mtmp.add(NWR.POS, posVal);
                        deg += "|POS:" + posVal;
                    }
                }
                generateTheMIdAndSetID(prdObj.getSpan(), mtmp);
                deg = "MentionId:" + mtmp.getID() + deg;

            }

	    /* 
	       iterate over the sub-tags <externalReferences> or <role>s of the <predicate>
	    */
            for (Object prdGObj : prdObj.getExternalReferencesOrRole()) {

		/*
		  process the <externalReferences> sub-tag: enrich the EVENT_MENTION related to <predicate>
		*/
                if (prdGObj instanceof ExternalReferences) {
                    boolean eventTypeFound = false;
                    if (predicatExtRef > 1) {
                        logWarn("more than one external ref for predicate:" + predicateID 
				+ " size: " + predicatExtRef);
                    }
                    predicatExtRef++;
                    for (ExternalRef exrObj : ((ExternalReferences) prdGObj).getExternalRef()) {
                        if (mtmp != null) {
			    // check 'resource' and 'reference' attributes
			    //
			    String resourceValue = exrObj.getResource();
			    String referenceValue = exrObj.getReference();
					
                            if (resourceValue != null) {
				
				// check cases different from resource="EventType"
				//
                                if (!resourceValue.equalsIgnoreCase("EventType")) {

				    URI resourceMappedValue = srlExternalRefResourceTypeMapper.get(resourceValue);
				    URIImpl valueURI;
				    if (resourceMappedValue != null) {
					valueURI = getUriForSrlExternalRefResource(resourceValue, referenceValue);
				    } else {
					// force dynamic URIs
					//
					resourceMappedValue = ValueFactoryImpl.getInstance()
					    .createURI(NWR.NAMESPACE, resourceValue.toLowerCase() + "Ref");
					valueURI = new URIImpl("http://www.newsreader-project.eu/" 
							       + resourceValue.toLowerCase() + "/" + referenceValue);
				    }
				    mtmp.add(resourceMappedValue, valueURI);
				    deg += "|" + resourceMappedValue + ":" + valueURI;
				} else { 
				    // resource="EventType" => check 'reference' attribute
				    //
				    URI referenceMappedValue = null;
				    if (referenceValue != null) { 
					referenceMappedValue = eventClassMapper.get(referenceValue);
				    } 
				    if (referenceMappedValue == null) {
					// force this default value
					//
					referenceMappedValue = NWR.EVENT_SPEECH_COGNITIVE;
				    }

				    mtmp.add(NWR.EVENT_CLASS, referenceMappedValue);
				    deg += "|EVENT_CLASS:" + referenceMappedValue;
				    eventTypeFound = true;
				}
			    } else {
				// resourceValue == null)
                                logWarn("xpath(//NAF/srl/predicate/externalReferences/externalRef@Resource(NULL)): predicateID("
                                        + predicateID + ")");
                            }
                        } else {
                            logWarn("Mapping error - Mention null - xpath(NAF/srl/predicate/externalReferences/externalRef): predicateID("
                                    + predicateID + ")");
                        }
                    }
                    if (!eventTypeFound) {
                        mtmp.add(NWR.EVENT_CLASS, eventClassMapper.get("contextual"));
                        deg += "|EVENT_CLASS:" + eventClassMapper.get("contextual");
                        eventTypeFound = true;
                    }
                    if (eventTypeFound) {
                        logDebug(deg);
                        int addedNew = addOrMergeAMention(mtmp);
                        
			logDebug("ROL1: <srl> <predicate> adding new event mention for id " 
				+ predicateID + ", charSpan |" + predicateCharSpan + "|");
			
                        if (addedNew==1){
                            srlMention2++;
                            srlMention++;
                        }
                        
                        entityMentions.addLast(mtmp);
                        
                        eventMentionId = mtmp.getID().stringValue();
                    } else {
                        // if eventType not found or no mapping for it
                        // write error and discard this mention
                        logWarn("Mention discarded for predicateID(" + predicateID + ") - ID("
                                + mtmp.getID().toString() + ")");
                    }
                } else 
		    /*
		      process the <role> sub-tag

		      rules:
		      A) if the semantic role of <role> is temporal expression ("AM-TMP"), then
		      two additional mentions are created: 
			 (1) a temporal-expression mention with span <role><span> (if not already extracted) 
			 (2) a TLINK between the event mention (<predicate><span>) and the temporal-expression mention (<role><span>)
		      B) else if the <role> <span> COINCIDES EXACTLY with a previously extracted mention
		      then create an single additional mention (a participation mention with span 
                      <predicate><span> + <role><span>)
		    */
		    if (prdGObj instanceof Role) {
			boolean MenCreated = false;
			String charS = null;
			String deg2 = "";
			LinkedList<Term> roleTermList = null;

			Span roleSpan = ((Role) prdGObj).getSpan();
			String roleCharSpan = getCharSpanFromSpan(roleSpan);
			boolean createTemporalexpressionMentionFlag = false;
			boolean createTlinkFlag = false;
			boolean createParticipationMentionFlag = false;

			String semRole = ((Role) prdGObj).getSemRole();
			if ((semRole != null) && (semRole.equalsIgnoreCase("AM-TMP"))) {
			    createTlinkFlag = true;
			    createTemporalexpressionMentionFlag = true;
			    logDebug(" ROL1: <srl> <role> found TLINK for |" + predicateCharSpan + "|" + roleCharSpan + "|");
			} else {
			    if (checkAlreadyAcceptedMention(roleCharSpan)) {
				createParticipationMentionFlag = true;
				logDebug(" ROL1: <srl> <role> found already existent mention for |" + roleCharSpan + "|");
			    }
			}


			/*
			  create a temporal-expression mention with span <role><span> if not already extracted
			*/
			if (createTemporalexpressionMentionFlag) {
			    if (! checkAlreadyAcceptedMention(roleCharSpan)) {
				Record roleM = Record.create();
				roleM.add(RDF.TYPE, NWR.TIME_MENTION, NWR.TIME_OR_EVENT_MENTION, 
					  NWR.ENTITY_MENTION, KS.MENTION);
				roleM.add(KS.MENTION_OF, news_file_id);

				//  compute the Term list for the <role>
				roleTermList = new LinkedList<Term>();
				for (Target rspnTar : ((Role) prdGObj).getSpan().getTarget()) {
				    Term ttmp = getTermfromTermId((Term) rspnTar.getId());
				    roleTermList.addLast(ttmp);
				}
			    
				// generate and set ID of the mention
				generateTheMIdAndSetID(roleSpan, roleM);

				// try to add the mention
				if(addOrMergeAMention(roleM) == 1) {
				    timeMention2++;
				}

				logDebug("   ROL1: created temporal-expression mention for |" + roleCharSpan + "|");
			    }
			}

			/*
			  create the new relation mention (either participation or TLINK) with the span of <predicate> + <role>
			  TODO:
			  - check the enrichment (externalReferences, TYPE, extent, ...)
			  - check SYNTACTIC_HEAD
			*/
			if (createParticipationMentionFlag || createTlinkFlag) {
			    Record relationM = Record.create();

			    /*
			      set as NWR.SOURCE of the relation mention the id of the EVENT_MENTION related to <predicate>
			    */
			    if (eventMentionId != null) {
				relationM.add(NWR.SOURCE, new URIImpl(eventMentionId));
				deg2 += "|SOURCE:" + eventMentionId;
			    } else {
				logWarn("//NAF/srl/predicate/role/ - a Role without Predicate roleID("
					+ ((Role) prdGObj).getId() + ")");
			    }

			    /*
			      set as NWR.TARGET of the relation mention the id of the role mention related to <role>
			    */
			    {
				URI roleId = getMentionIDFromCharSpan(roleCharSpan);
				relationM.add(NWR.TARGET, roleId);
			    }

			    /* 
			       set the RDF.TYPE
			    */
			    if (createParticipationMentionFlag) {
				relationM.add(RDF.TYPE, NWR.PARTICIPATION, 
					      NWR.RELATION_MENTION, KS.MENTION);
				deg2 = "|TYPE:PARTICIPATION,RELATION_MENTION,MENTION|" + deg2;
				relationM.add(NWR.THEMATIC_ROLE, ((Role) prdGObj).getSemRole());
				deg2 += "|THEMATIC_ROLE:" + ((Role) prdGObj).getSemRole();
			    } else {
				relationM.add(RDF.TYPE, NWR.TLINK, 
					      NWR.RELATION_MENTION, KS.MENTION);
				deg2 = "|TYPE:TLINK,RELATION_MENTION,MENTION|" + deg2;
			    }
			    relationM.add(KS.MENTION_OF, news_file_id);

			    /* 
			       generate and set ID of the relation mention
			    */
			    //  compute the Term list for the <role>
			    if (roleTermList == null) {
				roleTermList = new LinkedList<Term>();
				for (Target rspnTar : ((Role) prdGObj).getSpan().getTarget()) {
				    Term ttmp = getTermfromTermId((Term) rspnTar.getId());
				    roleTermList.addLast(ttmp);
				}
			    }
			    generateTheMIdAndSetID_forParticipationMention(eventTermList, roleTermList, relationM);
			    deg2 = "MentionId:" + relationM.getID() + deg2;
			    boolean create = false;

			    /* 
			       always add the relation mention (do not check if a previously accepted mention with the span of <role> exists)
			    */
			    int addedNew = -1;
			    MenCreated = true;

			    charS = relationM.getUnique(NIF.BEGIN_INDEX, Integer.class) + "," + relationM.getUnique(NIF.END_INDEX, Integer.class);
			    if (createParticipationMentionFlag) {
				logDebug("  ROL1: <srl> <predicate> <role> adding new participation mention for |" + charS + "|");
			    } else {
				logDebug("  ROL1: <srl> <predicate> <role> adding new TLINK mention for |" + charS + "|");
			    }

			    /* 
			       try to add the relation mention
			    */
			    addedNew = addOrMergeAMention(relationM);
			    if (addedNew==1){
				rolewithEntity++;
				rolewithEntity2++;
				roleMentions++;
			    } else {
				// if with entity and conflict or enriched, counted as discarded
				if(create){
				    rolewithoutEntity++;
				}
			    }

			    /* 
			       add the information from <externalReferences> sub-tag (the relation mention is always created)
			    */
			    for (Object roleGOBJ : ((Role) prdGObj).getExternalReferences()) {
				if (roleGOBJ instanceof ExternalReferences) {
				    for (ExternalRef rexRefObj : ((ExternalReferences) roleGOBJ).getExternalRef()) {

					// check 'resource' and 'reference' attributes
					//
					String resourceValue = rexRefObj.getResource();
					String referenceValue = rexRefObj.getReference();

					if (resourceValue != null) {
					    
					    URI resourceMappedValue = srlExternalRefResourceTypeMapper.get(resourceValue);
					    URIImpl valueURI;
					    if (resourceMappedValue != null) {
						valueURI = getUriForSrlExternalRefResource(resourceValue, referenceValue);
					    } else {
						// force dynamic URIs
						//
						resourceMappedValue = ValueFactoryImpl.getInstance()
						    .createURI(NWR.NAMESPACE, resourceValue.toLowerCase() + "Ref");
						valueURI = new URIImpl("http://www.newsreader-project.eu/" 
								       + resourceValue.toLowerCase() + "/" + referenceValue);
					    }
					    if (charS != null) {
						mentionListHash.get(charS).add(resourceMappedValue, valueURI);
					    }
					    deg2 += "|" + resourceMappedValue + ":" + valueURI;
					}  else {
					    // resourceValue == null)
					    logWarn("xpath(//NAF/srl/predicate/role/externalReferences/externalRef@Resource(NULL)): RoleID("
						    + ((Role) prdGObj).getId() + ")");
					}
				    }
				}
			    } // end of adding info from <externalReferences> within <role>
			}
			logDebug(deg2);
			
		    } // end of <role> processing

            } // end of iteration over <externalReferences> or <role>s of the <predicate>

        } // end of iteration over <predicate>
    }


    private static void getNAFHEADERMentions(NafHeader obj) {
        logDebug("Start reading the naf metadata:");
        String deg = "";
        Public publicProp = ((NafHeader) obj).getPublic();
        initURIIDS(publicProp);
        Record newsFile = Record.create();
        Record nafFile = Record.create();
        nafFile2 = nafFile;
        newsFile2 = newsFile;
        newsFile.setID(news_file_id);
        nafFile.setID(NAF_file_id);
        deg += "news_file_id:" + news_file_id;
        newsFile.add(RDF.TYPE, NWR.NEWS);
        deg += "\nNAF_file_id:" + NAF_file_id;

        nafFile.add(RDF.TYPE, NWR.NAFDOCUMENT);
        nafFile.add(NWR.ANNOTATION_OF, newsFile.getID());
        newsFile.add(NWR.ANNOTATED_WITH, nafFile.getID());
        if (doc.getVersion() != null) {
            nafFile.add(NWR.VERSION, doc.getVersion());
            deg += "\nVERSION:" + doc.getVersion();
        }

	/* 
	   set DCTERMS.SOURCE according to the News URI
	*/
	URIImpl sourceURL;
	if (PREFIX.matches("(?i)http://www.newsreader-project.eu/LNdata.*")) {
	    // LexisNexis news
	    String preStr = "http://www.lexisnexis.com/uk/nexis/docview/getDocForCuiReq?lni=";
	    String postStr = "&csi=138620&perma=true";
	    String srcUrlstr = new String(preStr + publicProp.getPublicId() + postStr);
	    sourceURL = new URIImpl(srcUrlstr);
	} else {
	    // non-LexisNexis news
	    sourceURL = (URIImpl)news_file_id;
	}
	newsFile.add(DCTERMS.SOURCE, sourceURL);
	
	if (publicProp.getPublicId() != null) {
            nafFile.add(DCTERMS.IDENTIFIER, publicProp.getPublicId());// NAF/nafHeader/public@publicId
            deg += "|IDENTIFIER:" + publicProp.getPublicId();
        }
        if (doc.getXmlLang() != null) {
            newsFile.add(DCTERMS.LANGUAGE, Data.languageCodeToURI(doc.getXmlLang()));
            deg += "|LANGUAGE:" + Data.languageCodeToURI(doc.getXmlLang());
        } else {
            logWarn("Language not catched:" + doc.getXmlLang());
        }
        FileDesc fileDesc = null;
        if (((NafHeader) obj).getFileDesc() != null) {
            fileDesc = ((NafHeader) obj).getFileDesc();
            if (fileDesc.getTitle() != null) {
                newsFile.add(DCTERMS.TITLE, fileDesc.getTitle());
                deg += "|TITLE:" + fileDesc.getTitle();
            }
            if (fileDesc.getAuthor() != null) {
                newsFile.add(DCTERMS.CREATOR, fileDesc.getAuthor());
                deg += "|Author:" + fileDesc.getAuthor();
            }
            if (fileDesc.getCreationtime() != null) {
                newsFile.add(DCTERMS.CREATED, fileDesc.getCreationtime());
                deg += "|Creationtime:" + fileDesc.getCreationtime();
            }
            if (fileDesc.getFilename() != null) {
                newsFile.add(NWR.ORIGINAL_FILE_NAME, fileDesc.getFilename());
                deg += "|Filename:" + fileDesc.getFilename();
            }
            if (fileDesc.getFiletype() != null) {
                newsFile.add(NWR.ORIGINAL_FILE_FORMAT, fileDesc.getFiletype());
                deg += "|Filetype:" + fileDesc.getFiletype();
            }
            if (fileDesc.getPages() != null) {
                newsFile.add(NWR.ORIGINAL_PAGES, fileDesc.getPages());
                deg += "|Pages:" + fileDesc.getPages();
            }
        } else {
            logWarn("FileDesc: null");
        }
        for (LinguisticProcessors lpObj : ((NafHeader) obj).getLinguisticProcessors()) {
            deg += "\n";
            if (lpObj.getLayer() != null) {
                if (nafLayerMapper.containsKey(lpObj.getLayer())
                        && nafLayerMapper.get(lpObj.getLayer()) != null) {
                    nafFile.add(NWR.LAYER, nafLayerMapper.get(lpObj.getLayer()));
                    deg += "LAYER:" + nafLayerMapper.get(lpObj.getLayer());
                } else {
                    logWarn("xpath(//NAF/nafHeader/linguisticProcessors/@layer["
                            + lpObj.getLayer() + "]),  unknown layer.");
                }
            }

            for (Lp lpO : lpObj.getLp()) {
                deg += "\n";
                Record r3 = Record.create();
                if (lpO.getName() != null) {
                    r3.add(DCTERMS.TITLE, lpO.getName());
                    deg += "TITLE:" + lpO.getName();
                }
                if (lpO.getVersion() != null) {
                    r3.add(NWR.VERSION, lpO.getVersion());
                    deg += "|VERSION:" + lpO.getVersion();
                }
                String namuri = java.net.URLEncoder.encode(lpO.getName());
                String uri = PREFIX + (PREFIX.endsWith("/") ? "" : "/") + "lp/" + namuri + "/"
                        + lpO.getVersion();

                URI rId = new URIImpl(uri);
                r3.setID(rId);
                nafFile.add(DCTERMS.CREATOR, r3);
                deg += "|CREATOR:" + r3;
            }
        }

        logDebug(deg);
    }


    private static void getCoreferencesMentions(Coreferences obj) {
        if (!checkHeaderTextTerms()) {
            logError("Error: populating interrupted");
        } else {
            logDebug("Start mapping the Coreferences mentions:");
        }
        String deg = "\n";
	
	/*
	  process the <coref> tag
	*/
        for (Coref corefObj : ((Coreferences) obj).getCoref()) {
            deg = "";
            if (corefObj.getSpan().size() < 1) {
                logWarn("Every coref must contain a 'span' element inside 'references'");
            }
	    
	    /*
	      if type exists and =="event" then create a new mention for each span;
	      otherwise create a new mention for each span only if at least one span includes (or is) a previously extracted mention.
	    */

	    boolean addMentionsFlag = false;

	    String corefType = corefObj.getType();
	    List<Object> typesOfIncludedMention = null;
	    boolean eventM = false;
	    if (corefType != null && corefType.equalsIgnoreCase("event")) {
		// create new mentions
		addMentionsFlag = true;
		eventM = true;
	    } else {
		// check if at least one span includes (or is) a previously extracted mention
		//
		for (Span corefSpan : corefObj.getSpan()) {
		    String corefCharSpan = getCharSpanFromSpan(corefSpan);
		    Object[] retArray = checkSpanIncludesAnAlreadyAcceptedMention(corefCharSpan);
		    int inclFlag = ((Integer) retArray[0]).intValue();
		    if ((inclFlag == 1) || (inclFlag == 2)) {
			addMentionsFlag = true;
			String includedMentionCharSpan = (String)retArray[1];
			typesOfIncludedMention = getMentionTypeFromCharSpan(includedMentionCharSpan);
			logDebug("ROL1: <coref> id " + corefObj.getId() + ": found included mention for |" 
				 + corefCharSpan + "|, included mention |" + includedMentionCharSpan 
				 + "|, inclFlag " + inclFlag
				 + ", types " + getTypeasString(typesOfIncludedMention));
			break;
		    }
		}
	    }

	    if (addMentionsFlag) {
		for (Span corefSpan : corefObj.getSpan()) {
		    String corefCharSpan = getCharSpanFromSpan(corefSpan);
		    if (checkAlreadyAcceptedMention(corefCharSpan)) {
			logDebug("ROL1: <coref> id " + corefObj.getId() + ": skipping already existent mention with charSpan |" 
				 + corefCharSpan + "|");
			continue;
		    }
		    deg = "";
		    Record m = Record.create();
		    m.add(KS.MENTION_OF, news_file_id);
		    deg += "MENTION_OF:" + news_file_id;
		    if (corefObj.getSpan().size() > 1) {
			m.add(NWR.LOCAL_COREF_ID, corefObj.getId());
			deg += "|LOCAL_COREF_ID:" + corefObj.getId();
		    }
		    
		    if (eventM) {
			m.add(RDF.TYPE, NWR.EVENT_MENTION, NWR.TIME_OR_EVENT_MENTION,
			      NWR.ENTITY_MENTION, KS.MENTION);
			deg = "|TYPE:EVENT_MENTION,TIME_OR_EVENT_MENTION,ENTITY_MENTION,ENTITY_MENTION"
                            + deg;
			eventM = true;
		    } else {
			m.add(RDF.TYPE, NWR.OBJECT_MENTION, NWR.ENTITY_MENTION, KS.MENTION);
			deg = "TYPE:,OBJECT_MENTION,ENTITY_MENTION,ENTITY_MENTION|" + deg;
			// add types of includedMention
			//
			if (typesOfIncludedMention != null) {
			    m.add(RDF.TYPE, typesOfIncludedMention);
			}
		    }
		    logDebug("ROL1: <coref> id " + corefObj.getId() + ": adding new mention with charSpan |" 
			    + corefCharSpan + "|, and type " + m.get(RDF.TYPE));

		    if (corefSpan.getTarget().size() < 1) {
			logWarn("Every span in an entity must contain at least one target inside");
		    }
		    for (Target spTar : corefSpan.getTarget()) {
			if (eventM) {
			    Term eventTerm = getTermfromTermId((Term) spTar.getId());
			    m.add(NWR.PRED, eventTerm.getLemma());
			    deg += "|PRED:" + eventTerm.getLemma();
			    if (eventTerm.getPos() != null) {
				URI posVal = (eventTerm.getPos().equals("V") || 
					      eventTerm.getPos().equals("N")) 
				    ? partOfSpeechMapper.get(eventTerm.getPos())
				    : partOfSpeechMapper.get("");
				m.add(NWR.POS, posVal);
				deg += "|POS:" + posVal;
			    } else {
				logWarn("//NAF/coreferences/coref/span/target/@id/@getPOS[null], id("
					+ eventTerm.getId() + ")");
			    }
			}
			if (!eventM && spTar.getHead() != null && spTar.getHead().equals("yes")) {
			    if (spTar.getId() != null) {
				m.add(NWR.SYNTACTIC_HEAD, spTar.getId());
				deg += "|SYNTACTIC_HEAD:" + spTar.getId();
			    } else {
				logWarn("//NAF/coreferences/coref/span/target[@head='yes']/@id[null], id("
					+ spTar.getId() + ")");
			    }
			}
		    }
		    generateTheMIdAndSetID(corefSpan, m);
		    deg = "MentionId:" + m.getID() + deg;
		    logDebug(deg);

		    int addedNew = addOrMergeAMention(m);
                
		    if (!eventM){
			// eventM == false
			if(addedNew==1){
			    corefMention2++;
			    no_mapping++;
			    corefMentionNotEvent++;
			    corefMention++;
			    // logWarn("xpath(//NAF/coreferences/coref/) add a new object mention, missing type.");
			}
		    } else {
			// eventM == true
			if(addedNew==1){
			    srlMention2++;
			    corefMentionEvent++;
			    corefMention++;
			}
		    }
		    entityMentions.addLast(m);
		}
	    } else {
		logDebug("ROL1: <coref> id " + corefObj.getId() + ": entirely skipped, NO included mentions");
	    }
        }
    }


    private static LinkedList<Term> mergeTwoTermLists(LinkedList<Term> eventTermList,
            LinkedList<Term> roleTermList) {

        LinkedList<Term> merged = new LinkedList<Term>();
	/* 
	   first the eventTermList (from <predicate>) then roleTermList (from <role>)
	 */
	for (Term evn : eventTermList) {
	    merged.addLast(evn);
	}
	for (Term rol : roleTermList) {
	    merged.addLast(rol);
	}
	logDebug("Two lists merged: eventTermListSize(" + eventTermList.size()
		 + ") + roleTermListSize(" + roleTermList.size() + ") = mergedListSize("
		 + merged.size() + ").");
	return merged;
    }

    // given a charSpan (e.g. "321,325") check if there is an already accepted mention with such span
    //
    private static boolean checkAlreadyAcceptedMention(String charSpan) {
	return mentionListHash.containsKey(charSpan);
    }


    // given a charSpan (e.g. "321,325") check if it includes an already accepted mention; 
    // return an array of 3 Objects: (Integer)inclFlag, (String)outCharSpan where
    //   0, null                    if there are no already-accepted mentions included in the charSpan
    //   1, charSpan                if an already-accepted mention coincides with the charSpan
    //   2, chSpanOfIncludedMention if an already-accepted mention is stricly included in the charSpan
    //
    private static Object[] checkSpanIncludesAnAlreadyAcceptedMention(String charSpan) {
	Object[] retArray = new Object[2];
	// check if an already-accepted mention coincides
	if (checkAlreadyAcceptedMention(charSpan)) {
	    retArray[0] = new Integer(1);
	    retArray[1] = charSpan;
	    return retArray;
	}

	// check if an already-accepted mention is strictly included
	String[] fields = charSpan.split(",");
	int spanBeginC = Integer.parseInt(fields[0]);
	int spanEndC   = Integer.parseInt(fields[1]);

	Enumeration keys = mentionListHash.keys();
	String[] kfields;
	int kBeginC;
	int kEndC;
	while( keys.hasMoreElements() ) {
	    String key = (String) keys.nextElement();
	    kfields = key.split(",");
	    kBeginC = Integer.parseInt(kfields[0]);
	    kEndC   = Integer.parseInt(kfields[1]);
	    if ((kBeginC >= spanBeginC) && (kEndC <= spanEndC)) {
		retArray[0] = new Integer(2);
		retArray[1] = key;
		return retArray;
	    }
	}
	// no already-accepted mentions included
	retArray[0] = new Integer(0);
	retArray[1] = null;
	return retArray;
    }

    // given a Span object return its charSpan (e.g. "321,325")
    //
    private static String getCharSpanFromSpan(Span sp) {
        LinkedList<Wf> wordsL = fromSpanGetAllMentions(sp.getTarget());
        String begin = wordsL.getFirst().getOffset();
	Wf lastW = wordsL.getLast();
        int end = Integer.parseInt(lastW.getOffset()) + Integer.parseInt(lastW.getLength());
	String charSpan = begin + "," + Integer.toString(end);
	return charSpan;
    }

    // given a charSpan (e.g. "321,325") get the ID of the mention with such span if exists, otherwise return null
    //
    private static URI getMentionIDFromCharSpan(String charSpan) {
	if (mentionListHash.containsKey(charSpan)) {
	    return mentionListHash.get(charSpan).getID();
	} else {
	    return null;
	}
    }

    // given a charSpan (e.g. "321,325") get the type of the mention with such span if exists, otherwise return null
    //
    private static List<Object> getMentionTypeFromCharSpan(String charSpan) {
	if (mentionListHash.containsKey(charSpan)) {
	    return mentionListHash.get(charSpan).get(RDF.TYPE);
	} else {
	    return null;
	}
    }

    /*
      return:
	1  if input mention "m" was added as a new mention
	0  if a previously accepted mention with the same span of input mention m was enriched
        <0 in case of problems (e.g. due to a conflict with previously accepted mention); input mention m was not added 
    */
    private static Integer addOrMergeAMention(Record m) {
        String charS = m.getUnique(NIF.BEGIN_INDEX, Integer.class) + "," + m.getUnique(NIF.END_INDEX, Integer.class);
        if (mentionListHash.containsKey(charS)) {
	    /* 
	       there is a previously accepted mention with the same span: try to enrich it
	    */
            boolean chk = checkClassCompatibility(mentionListHash.get(charS), m);
            if (!chk) {
		/* 
		   there is conflict between the input mention and the previously accepted mention with the same span:
		   check if the new mention can replace the old one, otherwise report the error
		*/
		if (checkMentionReplaceability(mentionListHash.get(charS), m)){
		    // replace the old mention with the new one
		    mentionListHash.put(charS, m);
		    logWarn("Replacement with Mention: " + m.getID()+", class("+getTypeasString(m.get(RDF.TYPE))+")");
		    return 0;
		}

                String types =getTypeasString(m.get(RDF.TYPE));
                if(types.contains(NWR.PARTICIPATION.stringValue())){
                    logWarn("Participation collision error, mentionID("+m.getID()+") class1("+getTypeasString(m.get(RDF.TYPE))+"), class-pre-xtracted("+getTypeasString(mentionListHash.get(charS).get(RDF.TYPE))+")");
                }else{
                    logWarn("Generic collision error, mentionID("+m.getID()+") class1("+getTypeasString(m.get(RDF.TYPE))+"), class-pre-xtracted("+getTypeasString(mentionListHash.get(charS).get(RDF.TYPE))+")");
                }
                return -1;
            } else {
		/* 
		   there is compatibility between the input mention and the previously accepted mention with the same span:
		   enrich old mention with properties from the new one (except participation mentions)
		*/
                String types =getTypeasString(m.get(RDF.TYPE));
                if(types.contains(NWR.PARTICIPATION.stringValue())){//Rule: no enrichment for participation
                    logWarn("Refused enrichment with participation mention, mentionID("+m.getID()+")");
                    return -1;
                }
		// enrich mention
		ListIterator<URI> mit = m.getProperties().listIterator();
		while (mit.hasNext()) {
		    URI mittmp = mit.next();
		    
		    for (Object pit : m.get(mittmp)) {
			mentionListHash.get(charS).add(mittmp, pit);
		    }
		}
		logWarn("Mention enrichment: " + m.getID()+", class("+getTypeasString(m.get(RDF.TYPE))+")");
		return 0;
            }
            
        } else {
	    /* 
	       the mention is new (there is no previously accepted mention with the same span)
	    */
	    mentionListHash.put(charS, m);
	    logDebug("Created Mention: " + m.getID());
	    return 1;
	}
    }

    // apply to all the mentions the following changes:
    //  - add the "extent" attribute as NIF.ANCHOR_OF
    //
    private static void fixMentions() {
	Enumeration keys = mentionListHash.keys();
	while( keys.hasMoreElements() ) {
	    String key = (String) keys.nextElement();
	    Record m = (Record) mentionListHash.get(key);

	    // get charStartIndex and charEndIndex from the mention charSpan (= the key)
	    //
	    String[] csList = key.split(",");
	    int cStart = Integer.parseInt(csList[0]);
	    int cEnd = Integer.parseInt(csList[1]);
	    String extentStr = rawText.substring(cStart, cEnd);
	    m.add(NIF.ANCHOR_OF, extentStr);
	}
    }

    private static String getTypeasString(List<Object> list) {
        String tmp="";
        for(Object ll :list){
            tmp+=ll.toString()+",";
        }
        tmp+="\"";
        return tmp.replace(",\"","");
    }

    private static boolean checkClassCompatibility(Record m, Record m2) {
        List<Object> types = m.get(RDF.TYPE);
        List<Object> types1 = m2.get(RDF.TYPE);
        for (Object tytmp : types) {
            if (!types1.contains(tytmp)) {
                return false;
            }
        }
        return true;
    }

    private static boolean checkMentionReplaceability(Record oldM, Record newM) {
	/* 
	   return true oldM can be replaced with newM;
	   this happens if oldM is a generic OBJECT_MENTION (= without ENTITY_TYPE) 
	   and newM is more specific (an OBJECT_MENTION with ENTITY_TYPE, or a TIME_MENTION, or EVENT_MENTION)
	*/
        List<Object> typesOld = oldM.get(RDF.TYPE);
        List<Object> typesNew = newM.get(RDF.TYPE);
	boolean isGenericOldM = (typesOld.contains(NWR.OBJECT_MENTION) && (oldM.getUnique(NWR.ENTITY_TYPE) == null));
	boolean isSpecificNewM = ((typesNew.contains(NWR.OBJECT_MENTION) && (newM.getUnique(NWR.ENTITY_TYPE) != null))
				  || typesNew.contains(NWR.TIME_MENTION)
				  || typesNew.contains(NWR.EVENT_MENTION));
	/*
	  logWarn("ROL3: checkMentionReplaceability: " + getTypeasString(typesOld) + "| " + getTypeasString(typesNew));
	  logWarn("ROL3:    isGenericOldM " + isGenericOldM + "| isSpecificNewM " + isSpecificNewM);
	*/
	if (isGenericOldM && isSpecificNewM) {
	    return true;
	} else {
	    return false;
	}
    }

    private static void initURIIDS(Public publicProp) {
        if (publicProp.getPublicId() == null) {
            logError("Corrupted Naf file: PublicId in the Naf header is missed");
            System.exit(0);
        }
        nafPublicId = publicProp.getPublicId();
        String uri = publicProp.getUri();
        //TODO remove it @mohammed Sept2014PREFIX
        //uri = PREFIX+uri;
        news_file_id = new URIImpl(uri);
        String nafuri = uri + ".naf";
        NAF_file_id = new URIImpl(nafuri);
        logDebug("news_file_id: " + uri);
        logDebug("NAF_file_id: " + nafuri);

	// set the PREFIX given the news uri
	try {
	    URL nurl = new URL(uri);
	    Path p = Paths.get(nurl.getPath());
	    PREFIX = nurl.getProtocol() + "://" + nurl.getAuthority() + "/" + p.subpath(0,2);
	} catch (Exception me) {
	    PREFIX = news_file_id.getNamespace();
	}
        logDebug("PREFIX: " + PREFIX);
    }

    static void generateMIDAndSetIdWF(LinkedList<Wf> wordsL, Record m) {
        int begin = Integer.parseInt(wordsL.getFirst().getOffset());
        int end = (Integer.parseInt(wordsL.getLast().getOffset()) + Integer.parseInt(wordsL
                .getLast().getLength()));
        m.add(NIF.BEGIN_INDEX, begin);
        m.add(NIF.END_INDEX, end);
        String tmpid = news_file_id + "#char=" + begin + "," + end;
        URI mId = new URIImpl(tmpid);
        m.setID(mId);
    }

    private static void logError(String error) {
        if (logErrorActive) {
            logger.error(filePath.getName() + " " + error);
        }
        if (!storePartialInforInCaseOfError) {
            System.exit(-1);
        }

    }

    private static void logDebug(String error) {
        if (logDebugActive) {
            logger.debug(error);
        }
    }
    private static void logWarn(String error) {
        logger.warn(filePath.getName() + " "+error);
    }

    private static boolean checkHeaderTextTerms() {
        if (globalTerms == null) {
            logWarn("Error: No term(s) has been catched!");
            return false;
        }
        if (globalText == null) {
            logWarn("Error: No text(s) has been catched!");
            return false;
        }
        return true;
    }

    public static Logger getLogger() {
        return logger;
    }

    public static void setLogger(final Logger logger) {
        processNAF.logger = logger;
    }

    private static void generateTheMIdAndSetID(Span spansObj, Record m) {
        LinkedList<Wf> wordsL = fromSpanGetAllMentions(((Span) spansObj).getTarget());
        int begin = Integer.parseInt(wordsL.getFirst().getOffset());
        int end = (Integer.parseInt(wordsL.getLast().getOffset()) + Integer.parseInt(wordsL
                .getLast().getLength()));
        m.add(NIF.BEGIN_INDEX, begin);
        m.add(NIF.END_INDEX, end);
        String muri = news_file_id + "#char=" + begin + "," + end;
        URI mId = new URIImpl(muri);
        m.setID(mId);
    }

    /* 
       similar to generateTheMIdAndSetID() but specific for ParticipationMention
     */
    private static void generateTheMIdAndSetID_forParticipationMention(LinkedList<Term> eventTermList, 
								       LinkedList<Term> roleTermList, 
								       Record m) {
	LinkedList<Wf> eventWordList = getTheWFListByThereTermsFromTargetList(eventTermList);
	LinkedList<Wf> roleWordList = getTheWFListByThereTermsFromTargetList(roleTermList);

        int charStartOfEvent = Integer.parseInt(eventWordList.getFirst().getOffset());
        int charEndOfEvent = Integer.parseInt(eventWordList.getLast().getOffset()) + Integer.parseInt(eventWordList.getLast().getLength());
        int charStartOfRole = Integer.parseInt(roleWordList.getFirst().getOffset());
        int charEndOfRole = Integer.parseInt(roleWordList.getLast().getOffset()) + Integer.parseInt(roleWordList.getLast().getLength());
	
	int beginIndex, endIndex;
	if (charStartOfEvent < charStartOfRole) {
	    beginIndex = charStartOfEvent;
	    endIndex = charEndOfRole;
	} else {
	    beginIndex = charStartOfRole;
	    endIndex = charEndOfEvent;
	}
        m.add(NIF.BEGIN_INDEX, beginIndex);
        m.add(NIF.END_INDEX, endIndex);
	
        String muri = news_file_id + "#char=" + beginIndex + "," + endIndex;
        URI mId = new URIImpl(muri);
        m.setID(mId);
    }


    /* 
       similar to generateTheMIdAndSetID() but specific for ParticipationMention
     */
    private static String getExtentOfParticipationMention(LinkedList<Term> eventTermList, 
							  LinkedList<Term> roleTermList) {
	LinkedList<Wf> eventWordList = getTheWFListByThereTermsFromTargetList(eventTermList);
	LinkedList<Wf> roleWordList = getTheWFListByThereTermsFromTargetList(roleTermList);

	LinkedList<Wf> mergedWordList = new LinkedList<Wf>();

	int charStartOfEvent = Integer.parseInt(eventWordList.getFirst().getOffset());
        int charStartOfRole = Integer.parseInt(roleWordList.getFirst().getOffset());

	LinkedList<Wf> firstWL, secondWL;
	if (charStartOfEvent <= charStartOfRole) {
	    firstWL = eventWordList;
	    secondWL = roleWordList;
	} else {
	    firstWL = roleWordList;
	    secondWL = eventWordList;
	}
	for (Wf w : firstWL) {
	    if (! mergedWordList.contains(w)) {mergedWordList.add(w);}
	}
	for (Wf w : secondWL) {
	    if (! mergedWordList.contains(w)) {mergedWordList.add(w);}
	}
	
	StringBuffer extent = new StringBuffer();
	for (Wf w : mergedWordList) {
	    extent.append(w.getvalue() + " ");
	}
	String sExtent = extent.toString();
	return sExtent.substring(0, sExtent.length() - 1);
    }


    /*
      return true if another mention exists with the same span
    */
    private static boolean checkDuplicate(String muri) {
        boolean re = false;
        Iterator<Record> ml = entityMentions.iterator();
        while (ml.hasNext()) {
            Record mtmp = ml.next();
            String charS2 = mtmp.getUnique(NIF.BEGIN_INDEX, Integer.class) + "," + mtmp.getUnique(NIF.END_INDEX, Integer.class);
            if (charS2.equals(muri)) {
            //if (mtmp.getID().stringValue().equals(muri)) {
                re = true;
		break;
            }
        }
        return re;
    }

    private static Term getTermfromTermId(Term termId) {
        if (globalTerms != null) {
            if (globalTerms.getTerm().contains(termId))
                return globalTerms.getTerm().get(globalTerms.getTerm().indexOf(termId));
        } else {
            Iterator<Object> ls = doc
                    .getNafHeaderOrRawOrTextOrTermsOrDepsOrChunksOrEntitiesOrCoreferencesOrConstituencyOrTimeExpressionsOrFactualitylayerOrTunitsOrLocationsOrDates()
                    .iterator();
            while (ls.hasNext()) {
                Object ltmp = ls.next();
                if (ltmp instanceof Terms) {
                    if (((Terms) ltmp).getTerm().contains(termId))
                        return globalTerms.getTerm().get(globalTerms.getTerm().indexOf(termId));
                    break;
                }
            }

        }
        logWarn("Term is not found, searched TermId(" + termId.getId() + ")");
        return null;
    }

    private static LinkedList<Wf> fromSpanGetAllMentionsTmx(List<Target> list) {
        LinkedList<Wf> returned = new LinkedList<Wf>();
        LinkedList<Wf> wordsIDL = new LinkedList<Wf>();
        for (Target ltmp : list) {
            wordsIDL.addLast((Wf) ltmp.getId());
        }
        if (globalText != null) {

            int found = 0;
            for (Wf wftmp : globalText.getWf()) {
                if (wordsIDL.contains(wftmp)) {
                    returned.addLast(wftmp);
                    found++;
                }
                if (found >= wordsIDL.size()) {
                    break;
                }
            }
        } else {
            Iterator<Object> doci2 = doc
                    .getNafHeaderOrRawOrTextOrTermsOrDepsOrChunksOrEntitiesOrCoreferencesOrConstituencyOrTimeExpressionsOrFactualitylayerOrTunitsOrLocationsOrDates()
                    .iterator();
            while (doci2.hasNext()) {
                Object prop = doci2.next();
                if (prop instanceof Text) {
                    int found = 0;
                    for (Wf wftmp : ((Text) prop).getWf()) {
                        if (wordsIDL.contains(wftmp)) {
                            returned.addLast(wftmp);
                            found++;
                        }
                        if (found >= wordsIDL.size()) {
                            break;
                        }
                    }
                }
            }
        }
        return returned;
    }

    private static void init() {
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
        entityMentions = new LinkedList<Record>();

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

    private static LinkedList<Wf> getTheWFListByThereTermsFromTargetList(LinkedList<Term> targetTermList) {
        LinkedList<Wf> returned = new LinkedList<Wf>();
        LinkedList<Wf> wordsIDL = new LinkedList<Wf>();
        boolean spanTermFound = false;
        if (globalTerms != null) {

            for (Term termtmp : globalTerms.getTerm()) {
                if (targetTermList.contains(termtmp)) {
                    Iterator<Object> spansl = termtmp
                            .getSentimentOrSpanOrExternalReferencesOrComponent().iterator();
                    while (spansl.hasNext()) {
                        Object spantmp = spansl.next();
                        if (spantmp instanceof Span) {
                            spanTermFound = true;
                            for (Target targtmp : ((Span) spantmp).getTarget()) {
                                wordsIDL.addLast((Wf) targtmp.getId());
                            }
                        }
                    }
                }
            }

        } else {
            Iterator<Object> doci = doc
                    .getNafHeaderOrRawOrTextOrTermsOrDepsOrChunksOrEntitiesOrCoreferencesOrConstituencyOrTimeExpressionsOrFactualitylayerOrTunitsOrLocationsOrDates()
                    .iterator();
            while (doci.hasNext()) {
                Object prop = doci.next();
                if (prop instanceof Terms) {
                    for (Term termtmp : ((Terms) prop).getTerm()) {
                        if (targetTermList.contains(termtmp)) {
                            Iterator<Object> spansl = termtmp
                                    .getSentimentOrSpanOrExternalReferencesOrComponent()
                                    .iterator();
                            while (spansl.hasNext()) {
                                Object spantmp = spansl.next();
                                if (spantmp instanceof Span) {
                                    spanTermFound = true;
                                    for (Target targtmp : ((Span) spantmp).getTarget()) {
                                        wordsIDL.addLast((Wf) targtmp.getId());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        if (!spanTermFound) {
            logWarn("Inconsistence NAF file(#TS): Every term must contain a span element");
        }
        if (globalText != null) {

            int found = 0;
            for (Wf wftmp : globalText.getWf()) {
                if (wordsIDL.contains(wftmp)) {
                    returned.addLast(wftmp);
                    found++;
                }
                if (found >= wordsIDL.size()) {
                    break;
                }
            }

            if (found < wordsIDL.size()) {
                logWarn("Inconsistence NAF file(#SW): Wf(s)  arenot found when loading term ");
            }

        } else {
            Iterator<Object> doci2 = doc
                    .getNafHeaderOrRawOrTextOrTermsOrDepsOrChunksOrEntitiesOrCoreferencesOrConstituencyOrTimeExpressionsOrFactualitylayerOrTunitsOrLocationsOrDates()
                    .iterator();
            int found = 0;
            while (doci2.hasNext()) {
                Object prop = doci2.next();
                if (prop instanceof Text) {

                    for (Wf wftmp : ((Text) prop).getWf()) {
                        if (wordsIDL.contains(wftmp)) {
                            returned.addLast(wftmp);
                            found++;
                        }
                        if (found >= wordsIDL.size()) {
                            break;
                        }

                    }
                }
            }
            if (found < wordsIDL.size()) {
                logWarn("Inconsistence NAF file(#SW): Wf(s)  arenot found when loading term ");
            }

        }
        return returned;
    }

    private static LinkedList<Wf> fromSpanGetAllMentions(List<Target> list) {

        LinkedList<Term> targetTermList = new LinkedList<Term>();
        Iterator<Target> targetList = list.iterator();
        while (targetList.hasNext()) {
            Target tarm = targetList.next();
            targetTermList.add((Term)tarm.getId());
        }
        LinkedList<Wf> corrispondingWf = getTheWFListByThereTermsFromTargetList(targetTermList);
        return corrispondingWf;
    }

    public static void readNAFFile(File naf) {

        try {
            JAXBContext jc = JAXBContext.newInstance("eu.fbk.knowledgestore.populator.naf.model");
            Unmarshaller unmarshaller = jc.createUnmarshaller();
            doc = (NAF) unmarshaller.unmarshal(new InputStreamReader(new FileInputStream(naf),
                    "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            logError(e.getMessage());
        } catch (FileNotFoundException e) {
            logError(e.getMessage());
        } catch (JAXBException e) {
            logError(e.getMessage());
        }

    }

    static void calculateMemory() {
        int mb = 1024 * 1024;

        // Getting the runtime reference from system
        Runtime runtime = Runtime.getRuntime();

        System.err.println("##### Heap utilization statistics [MB] #####");

        // Print used memory
        System.err.println("Used Memory:" + (runtime.totalMemory() - runtime.freeMemory()) / mb);

        // Print free memory
        System.err.println("Free Memory:" + runtime.freeMemory() / mb);

        // Print total available memory
        System.err.println("Total Memory:" + runtime.totalMemory() / mb);

        // Print Maximum available memory
        System.err.println("Max Memory:" + runtime.maxMemory() / mb);
    }
}
