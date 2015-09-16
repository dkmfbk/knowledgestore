package eu.fbk.knowledgestore.populator.naf;

import com.google.common.io.ByteStreams;
import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.populator.naf.model.*;
import eu.fbk.knowledgestore.vocabulary.KS;
import eu.fbk.knowledgestore.vocabulary.NIF;
import eu.fbk.knowledgestore.vocabulary.NWR;
import eu.fbk.rdfpro.util.IO;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.DCTERMS;
import org.openrdf.model.vocabulary.RDF;
import org.slf4j.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.*;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class processNAF {


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
    	processNAFVariables vars = new processNAFVariables();

        analyzePathAndRunSystem(path, disabled_Items, vars);
    }

    public static KSPresentation init(String fPath, Writer inout, String disabled_Items,
            boolean store_partical_info) throws JAXBException, IOException {
    	processNAFVariables vars = new processNAFVariables();
    	vars.storePartialInforInCaseOfError = store_partical_info;
    	vars.out = inout;
        statistics stat = readFile(fPath, disabled_Items, vars);
        KSPresentation returned = new KSPresentation();
        returned.setNaf_file_path(fPath);
        returned.setNews(vars.rawText);
        returned.setMentions(vars.mentionListHash);
        returned.setNaf(vars.nafFile2);
        returned.setNewsResource(vars.newsFile2);
        returned.setStats(stat);
        return returned;
    }

    
    private static void analyzePathAndRunSystem(String path, String disabled_Items,processNAFVariables vars)
            throws JAXBException, IOException {
    	vars.filePath = new File(path);
        if (vars.filePath.exists() && vars.filePath.isDirectory()) {
            // create report file in the same directory of running the system
        	vars.out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(
            		vars.filePath.getPath(), "report.txt")), "utf-8"));
            File[] listOfFiles = vars.filePath.listFiles();
            for (int i = 0; i < listOfFiles.length; i++) {
                if (listOfFiles[i].isFile() && listOfFiles[i].getName().endsWith(".naf")) {
                    System.err.println(i + "=" + listOfFiles[i].getName());
                    vars.out.append("\n" + i + "=" + listOfFiles[i].getName() + "\n");
                    readFile(listOfFiles[i].getPath(), disabled_Items,vars);
                }
                vars.out.flush();
                System.gc();
                Runtime.getRuntime().gc();
            }
        } else if (vars.filePath.exists() && vars.filePath.isFile()) {
            // create report file in the same directory of running the system
        	vars.out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(
        			vars.filePath.getPath() + ".report.txt")), "utf-8"));
        	vars.out.append(vars.filePath.getPath() + "\n");
            readFile(vars.filePath.getPath(), disabled_Items,vars);
        }
        vars.out.flush();
        vars.out.close();
    }

    public static statistics readFile(String filepath, String disabled_Items, processNAFVariables vars) throws JAXBException,
            IOException {
    	vars.storePartialInforInCaseOfError = true;
    	vars.filePath = new File(filepath);
        logDebug("Start working with (" + vars.filePath.getName() + ")",vars);
        String disabledItems = "";// Default empty so generate all layers of data
        if (disabled_Items != null
                && (disabled_Items.matches("(?i)Entity") || disabled_Items.contains("(?i)Mention") || disabled_Items.contains("(?i)Resource"))) {
            disabledItems = disabled_Items;
            logDebug("Disable layer: " + disabledItems,vars);
        }
        		readNAFFile(vars.filePath,vars);
        		getNAFHEADERMentions(vars.doc.getNafHeader(),vars);
            	vars.rawText = vars.doc.getRaw().getvalue();
            	vars.globalText = vars.doc.getText();
            	vars.globalTerms = vars.doc.getTerms();
            	getEntitiesMentions(vars.doc.getEntities(), disabledItems,vars);
                getCoreferencesMentions(vars.doc.getCoreferences(),vars);
                getTimeExpressionsMentions(vars.doc.getTimeExpressions(),vars);
              //  getFactualityMentions(vars.doc.getFactualitylayer(),vars);
                getFactualityMentionsV3(vars.doc.getFactualities(),vars);
                getSRLMentions(vars);
                getCLinksMentions(vars.doc.getCausalRelations(),vars);
                getTLinksMentions(vars.doc.getTemporalRelations(),vars);
	// logDebug("ROL1 before dumpStack()") ; Thread.currentThread().dumpStack();

	fixMentions(vars);

        logDebug("End of NAF populating.",vars);
        statistics st = new statistics();
        st.setObjectMention((vars.corefMention2+vars.entityMen2));
        st.setPER(vars.PER);
        st.setORG(vars.ORG);
        st.setLOC(vars.LOC);
        st.setFin(vars.fin);
        st.setMix(vars.mix);
        st.setPRO(vars.PRO);
        st.setNo_mapping(vars.no_mapping);
        st.setTimeMention(vars.timeMention2);
        st.setEventMention((vars.factualityMentions2 + vars.srlMention2));
        st.setParticipationMention(vars.rolewithEntity2);
        st.setEntity(vars.entityMen);
        st.setCoref(vars.corefMention);
        st.setCorefMentionEvent(vars.corefMentionEvent);
        st.setCorefMentionNotEvent(vars.corefMentionNotEvent);
        st.setFactuality(vars.factualityMentions);
        st.setRole(vars.roleMentions);
        st.setRolewithEntity(vars.rolewithEntity);
        st.setRolewithoutEntity(vars.rolewithoutEntity);
        st.setSrl(vars.srlMention);
        st.setTimex(vars.timeMention);
        st.setClinkMention(vars.clinkMentions);
        st.setClinkMentionDiscarded(vars.clinkMentionsDiscarded);
        st.setTlinkMention(vars.tlinkMentions);
        st.setTlinkMentionsEnriched(vars.tlinkMentionsEnriched);
        st.setTlinkMentionDiscarded(vars.tlinkMentionsDiscarded);
        logDebug(st.getStats(),vars);
        return st;
    }

    private static void getEntitiesMentions(Entities obj, String disabledItems,processNAFVariables vars) {
        if (!checkHeaderTextTerms(vars)) {
            logError("Error: populating stopped",vars);
        } else {
            logDebug("Start mapping the Entities mentions:",vars);
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
                        logDebug("Every entity must contain a 'span' element inside 'references'", vars);
                    }
                    if (((References) generalEntObj).getSpan().size() > 1) {
						logDebug("xpath(///NAF/entities/entity/references/span/), spanSize("
                                + ((References) generalEntObj).getSpan().size()
                                + ") Every entity must contain a unique 'span' element inside 'references'",vars);
                    }
                    for (Span spansObj : ((References) generalEntObj).getSpan()) {
			boolean addMentionFlag = true;

                        if (spansObj.getTarget().size() < 1) {
			    addMentionFlag = false;
                            logDebug("Every span in an entity must contain at least one target inside", vars);
			    continue;
                        }
                       
                        Record m = Record.create();
                        deg += "RDF.TYPE:OBJECT_MENTION,ENTITY_MENTION,MENTION";
                        m.add(RDF.TYPE, NWR.OBJECT_MENTION, NWR.ENTITY_MENTION, KS.MENTION);
                        deg = "MENTION_OF:" + vars.news_file_id.stringValue() + "|" + deg;
                        m.add(KS.MENTION_OF, vars.news_file_id);

                        if (((References) generalEntObj).getSpan().size() > 1) {
                            m.add(NWR.LOCAL_COREF_ID, entObj.getId());
                            deg += "|LOCAL_COREF_ID:" + entObj.getId();
                        }
                        generateTheMIdAndSetID(spansObj, m,vars);
                        charS = m.getUnique(NIF.BEGIN_INDEX, Integer.class) + "," + m.getUnique(NIF.END_INDEX, Integer.class);
                        deg = "MentionId:" + m.getID() + "|" + deg;


			/*
			  don't use predefined types from guidelines but keep the mention with type provided in the NAF
			*/
			boolean keepEntityTypeProvidedByNaf = true,dbpedia=true;
			String type3charLC = "";
			if(!dbpedia){
			if(entObj.getType()!=null&&entObj.getType()!=""&&!entObj.getType().equalsIgnoreCase("misc")){
			 type3charLC = entObj.getType().substring(0, 3).toLowerCase();
			}else{
				type3charLC = "misc";
				entObj.setType("misc");
			}
			if (keepEntityTypeProvidedByNaf) {
			    URI dynamicTypeUri = ValueFactoryImpl.getInstance().createURI(NWR.NAMESPACE, "entity_type_" + entObj.getType().toLowerCase());
			    m.add(NWR.ENTITY_TYPE, dynamicTypeUri);
			    deg += "|ENTITY_TYPE:" + dynamicTypeUri;
			    logDebug("ROL1: <entity> added new mention for id " + entObj.getId() + ", charSpan |" 
				    + getCharSpanFromSpan(spansObj,vars) + "|, type " + dynamicTypeUri,vars);
			} else {
			    if (vars.entityTypeMapper.containsKey(type3charLC)
				&& vars.entityTypeMapper.get(type3charLC) != null) {
				m.add(NWR.ENTITY_TYPE, vars.entityTypeMapper.get(type3charLC));
				deg += "|ENTITY_TYPE:" + vars.entityTypeMapper.get(type3charLC);
				logDebug("ROL1: <entity> STRANGE added new mention for id " + entObj.getId() + ", charSpan |" 
					+ getCharSpanFromSpan(spansObj,vars) + "|, type " + vars.entityTypeMapper.get(type3charLC),vars);
			    } else {
				addMentionFlag = false;
					logDebug("xpath(//NAF/entities/entity/@type),type(" + entObj.getType() + "), id("
							+ entObj.getId() + ") NO mapping for it", vars);
				vars.no_mapping++;
			    }
			}
			 }//dbpedia finish
			else{
				if(entObj.getType()==null||entObj.getType().equals("")){
					type3charLC = "misc";
					entObj.setType("misc");
				}else{
					if(entObj.getType().toLowerCase().contains("per")||entObj.getType().toLowerCase().contains("dbpedia:person")){
						entObj.setType("person");
						type3charLC = entObj.getType().substring(0, 3).toLowerCase();
					}else if(entObj.getType().toLowerCase().contains("org")||entObj.getType().toLowerCase().contains("dbpedia:organisation")){
						entObj.setType("organization");
						type3charLC = entObj.getType().substring(0, 3).toLowerCase();
					} else if(entObj.getType().toLowerCase().contains("loc")||entObj.getType().toLowerCase().contains("DBpedia:Place")){
						entObj.setType("location");
						type3charLC = entObj.getType().substring(0, 3).toLowerCase();
					}else {
						entObj.setType("misc");
						type3charLC = "misc";
					}
				}
			    URI dynamicTypeUri = ValueFactoryImpl.getInstance().createURI(NWR.NAMESPACE, "entity_type_" + entObj.getType().toLowerCase());
			    m.add(NWR.ENTITY_TYPE, dynamicTypeUri);
			    deg += "|ENTITY_TYPE:" + dynamicTypeUri;
			    logDebug("ROL1: <entity> added new mention for id " + entObj.getId() + ", charSpan |" 
				    + getCharSpanFromSpan(spansObj,vars) + "|, type " + dynamicTypeUri,vars);

			}
			if (addMentionFlag) {
			    if (addOrMergeAMention(m,vars)==1) {
		            String charS2 = m.getUnique(NIF.BEGIN_INDEX, Integer.class) + "," + m.getUnique(NIF.END_INDEX, Integer.class);
		            vars.entityMentions.put(charS2, m);
				if (type3charLC.equalsIgnoreCase("PER"))
					vars.PER++;
				if (type3charLC.equalsIgnoreCase("LOC"))
					vars.LOC++;
				if (type3charLC.equalsIgnoreCase("ORG"))
					vars.ORG++;
				if (type3charLC.equalsIgnoreCase("PRO"))
					vars.PRO++;
				if (type3charLC.equalsIgnoreCase("fin"))
					vars.fin++;
				if (type3charLC.equalsIgnoreCase("mix")||type3charLC.equalsIgnoreCase("misc"))
					vars.mix++;
				vars.entityMen2++;
				vars.entityMen++;
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
			//modeactive is two filters should be applied to the externalRefs.
			//1. Language For each entity, group externalRef(s) by language (using reftype) and use the first nonempty set, using this sorting: en, es, it, nl.
			//2. Confidence Once the language is chosen, just take the externalRef having the higher confidence (and, of course, the language chosen).
			boolean modeactive=true; 
			
			// if POCUS "reranker value" get it as externalreference otherwise do the language/highconfidence check
			for (ExternalRef exRObj : ((ExternalReferences) generalEntObj) .getExternalRef()) {
				if(exRObj.getSource().equalsIgnoreCase("POCUS")){
				if (referencesElements < 1) {
					logDebug("Every entity must contain a 'references' element:not possible to add ExternalRef to null.", vars);
				continue;
			    }
			   // String resourceValue = exRObj.getResource();
			    String referenceValue = exRObj.getReference();
			    chosenReferenceValue = new String(referenceValue);
			    modeactive=false;
				}
			}
			
			if(modeactive){
				LinkedList<ExternalRef> exrEn = new LinkedList<ExternalRef>();
				LinkedList<ExternalRef> exrEs = new LinkedList<ExternalRef>();
				LinkedList<ExternalRef> exrIt = new LinkedList<ExternalRef>();
				LinkedList<ExternalRef> exrNl = new LinkedList<ExternalRef>();
				
				ExternalReferences obs = ((ExternalReferences) generalEntObj) ;
				for (ExternalRef exRObj : obs.getExternalRef()) {
					if(exRObj!=null)
					getAllLayersOfExternalReferences( modeactive, exrEn,  exRObj, exrEs, exrIt,  exrNl,vars);
				}
				if(exrEn.size()>0)
					chosenReferenceValue = new String(getHighConfidenceReferenceValue(exrEn));
				else if(exrEs.size()>0)
					chosenReferenceValue = new String(getHighConfidenceReferenceValue(exrEs));
				else if(exrIt.size()>0)
					chosenReferenceValue = new String(getHighConfidenceReferenceValue(exrIt));
				else 
					chosenReferenceValue = new String(getHighConfidenceReferenceValue(exrNl));

			}else if(false){ //don't use it for now, it gets the last externalRef as the chosen one
				for (ExternalRef exRObj : ((ExternalReferences) generalEntObj) .getExternalRef()) {
					
					if (referencesElements < 1) {
						logDebug("Every entity must contain a 'references' element:not possible to add ExternalRef to null.", vars);
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
			}
			

			URIImpl chosenReferenceURI = new URIImpl(chosenReferenceValue);
			if (charS != null&&vars.mentionListHash.get(charS)!=null&&vars.mentionListHash.get(charS).get(KS.REFERS_TO).size()==0){
				vars.mentionListHash.get(charS).add(KS.REFERS_TO, chosenReferenceURI);
				//TODO mohammed if there already decided an externalRef don't add new one.
				deg += "|REFERS_TO:" + chosenReferenceValue;
				vars.entityMentions.get(charS).add(KS.REFERS_TO, chosenReferenceURI);
			}else{
				if (charS != null&&vars.mentionListHash.get(charS)!=null)
				deg += "|REFERS_TO:" + vars.mentionListHash.get(charS).get(KS.REFERS_TO);

			}
                    }
                } // end of   if (generalEntObj instanceof ExternalReferences) {

            } // end of    for (Object generalEntObj  : entObj.getReferencesOrExternalReferences())

            logDebug(deg,vars);
            if (referencesElements < 1) {
				logDebug("Every entity must contain a 'references' element", vars);
            }
        }
    }

    private static void getAllLayersOfExternalReferences(boolean modeactive, LinkedList<ExternalRef> exrEn, ExternalRef exRObj, LinkedList<ExternalRef> exrEs, LinkedList<ExternalRef> exrIt, LinkedList<ExternalRef> exrNl,processNAFVariables vars) {
			
			
		if(modeactive){
			if(exRObj.getReftype()!=null){
				switch(exRObj.getReftype()){
				case "en": exrEn.addLast(exRObj); break;
				case "es": exrEs.addLast(exRObj); break;
				case "it": exrIt.addLast(exRObj); break;
				case "nl": exrNl.addLast(exRObj); break;
				}
			}else if(exRObj.getReference().contains("dbpedia")){
				
				if(exRObj.getReference().contains("://dbpedia.org")){
					exrEn.addLast(exRObj);
				}else if(exRObj.getReference().contains("://es.dbpedia.org")){
					exrEs.addLast(exRObj);
				}else if(exRObj.getReference().contains("://it.dbpedia.org")){
					exrIt.addLast(exRObj);
				}else {
					exrNl.addLast(exRObj);
				}
			}else{
				logDebug("Every entity must contain a 'references' element with type or DBpedia reference: not possible to add ExternalRef to null.", vars);
			}
		}
		
		if(exRObj!=null&&exRObj.getExternalRef()!=null && exRObj.getExternalRef().size()>0){
			for(ExternalRef ff :exRObj.getExternalRef()){
			getAllLayersOfExternalReferences( modeactive, exrEn,  ff, exrEs, exrIt,  exrNl,vars);
			}
		}else
			return;
	}

	private static String getHighConfidenceReferenceValue(
			LinkedList<ExternalRef> exrl) {
    	ExternalRef current=null;
    	Double dt=0.0;
		for(ExternalRef tmp:exrl){
			Double co=Double.parseDouble(tmp.getConfidence());
			if(current==null){
				current = tmp;
				dt=co;
			}else{
				if(co>dt){
					current = tmp;
					dt=co;
				}
			}
		}
		return current.getReference();
	}

	private static void getTimeExpressionsMentions(TimeExpressions obj,processNAFVariables vars) {
        if (!checkHeaderTextTerms(vars)) {
            logError("Error: populating interrupted",vars);
        } else {
            logDebug("Start mapping the TimeExpressions mentions:",vars);
        }
        for (Timex3 tmxObj : ((TimeExpressions) obj).getTimex3()) {

	    Span tmxSpan = tmxObj.getSpan();
	    String tmxTypeUC = tmxObj.getType().toUpperCase();
	    boolean keepTimeTypeProvidedByNaf = true;

	    // patch for timex3 without <span>
	    if ((tmxSpan == null) || (tmxSpan.getTarget().size() < 1)) {
		logDebug("skipping timex3 without span, id is " + tmxObj.getId(), vars);
		continue;
	    }

            String deg = "";
            Record m = Record.create();
            m.add(RDF.TYPE, NWR.TIME_MENTION, NWR.TIME_OR_EVENT_MENTION, NWR.ENTITY_MENTION,
                    KS.MENTION);
            deg += "|TYPE:TIME_MENTION,TIME_OR_EVENT_MENTION,ENTITY_MENTION,MENTION";
	    m.add(KS.MENTION_OF, vars.news_file_id);
            LinkedList<Wf> wordsL = fromSpanGetAllMentionsTmx(((Span) tmxSpan).getTarget(), vars);
            generateMIDAndSetIdWF(wordsL, m,vars);
            deg = "MentionId:" + m.getID() + deg;

	    if (keepTimeTypeProvidedByNaf) {
		URI dynamicTypeUri = ValueFactoryImpl.getInstance().createURI(NWR.NAMESPACE, "timex3_" + tmxTypeUC.toLowerCase());
                m.add(NWR.TIME_TYPE, dynamicTypeUri);
                deg += "|TIME_TYPE:" + dynamicTypeUri;
		logDebug("ROL1: <timex3> added new mention for id " + tmxObj.getId() + ", type " + dynamicTypeUri,vars);
	    } else {
		if (vars.timex3TypeMapper.containsKey(tmxTypeUC)
                    && vars.timex3TypeMapper.get(tmxTypeUC) != null) {
		    m.add(NWR.TIME_TYPE, vars.timex3TypeMapper.get(tmxTypeUC));
		    deg += "|TIME_TYPE:" + vars.timex3TypeMapper.get(tmxTypeUC);
		    logDebug("ROL1: <timex3> STRANGE added new mention for id " + tmxObj.getId() 
			    + ", type " + vars.timex3TypeMapper.get(tmxTypeUC),vars);
		} else {
			logDebug("xpath(//NAF/timeExpressions/timex3/@type), type(" + tmxTypeUC
					+ "), No mapping.", vars);
		}
	    }
            if (false&&tmxObj.getBeginPoint() != null) { //TODO
                m.add(NWR.BEGIN_POINT, tmxObj.getBeginPoint());
                deg += "|BEGIN_POINT:" + tmxObj.getBeginPoint();
            }
	    if (false&&tmxObj.getEndPoint() != null) {
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
            logDebug(deg,vars);
            int addedNew = addOrMergeAMention(m,vars);
            if (addedNew==1){
            	vars.timeMention2++;
            	vars.timeMention++;
            }
            String charS2 = m.getUnique(NIF.BEGIN_INDEX, Integer.class) + "," + m.getUnique(NIF.END_INDEX, Integer.class);
            vars.entityMentions.put(charS2,m);
        }
    }

    private static void getFactualityMentions(Factualitylayer obj,processNAFVariables vars) {
        if (!checkHeaderTextTerms(vars)) {
            logError("Error: populating interrupted",vars);
        } else {
            logDebug("Start mapping the Factuality mentions:",vars);
        }
        for (Factvalue fvObj : ((Factualitylayer) obj).getFactvalue()) {
            String deg = "";
            Record m = Record.create();
            m.add(RDF.TYPE, NWR.EVENT_MENTION, NWR.TIME_OR_EVENT_MENTION, NWR.ENTITY_MENTION,
                    KS.MENTION);
            deg += "|TYPE:EVENT_MENTION,TIME_OR_EVENT_MENTION,ENTITY_MENTION,MENTION";
	    m.add(KS.MENTION_OF, vars.news_file_id);
            LinkedList<Target> tarlist = new LinkedList<Target>();
            Target tmp = new Target();
            tmp.setId(fvObj.getId());
            tarlist.addLast(tmp);
            LinkedList<Wf> wordsL = fromSpanGetAllMentionsTmx(tarlist,vars);
            generateMIDAndSetIdWF(wordsL, m,vars);
            deg = "MentionId:" + m.getID() + deg;
            // fvObj.getPrediction(); //TODO we need to verify the mapping here
            // m.add(NWR.CERTAINTY, fvObj.getPrediction());
            // m.add(NWR.FACTUALITY, fvObj.getPrediction());
            if (fvObj.getConfidence() != null) {
                m.add(NWR.FACTUALITY_CONFIDENCE, fvObj.getConfidence());
                deg += "|FACTUALITY_CONFIDENCE:" + fvObj.getConfidence();
            }
            logDebug(deg,vars);
            int addedNew = addOrMergeAMention(m,vars);
            if (addedNew==1){
            	vars.factualityMentions2++;
            	vars.factualityMentions++;
            }
            String charS2 = m.getUnique(NIF.BEGIN_INDEX, Integer.class) + "," + m.getUnique(NIF.END_INDEX, Integer.class);
            vars.entityMentions.put(charS2,m);

        }

    }

    
    private static void getFactualityMentionsV3(Factualities factualities,processNAFVariables vars) {
        if (!checkHeaderTextTerms(vars)) {
            logError("Error: populating interrupted",vars);
        } else {
            logDebug("Start mapping the Factualities mentions:",vars);
        }
        for (Factuality fvObj : factualities.getFactuality()) {

            String deg = "";
            Record m = Record.create();
            m.add(RDF.TYPE, NWR.EVENT_MENTION, NWR.TIME_OR_EVENT_MENTION, NWR.ENTITY_MENTION,
                    KS.MENTION);
            deg += "|TYPE:EVENT_MENTION,TIME_OR_EVENT_MENTION,ENTITY_MENTION,MENTION";
            m.add(KS.MENTION_OF, vars.news_file_id);
            LinkedList<Target> tarlist = new LinkedList<Target>();
           for(Target t : fvObj.getSpan().getTarget()){
            tarlist.addLast(t);
           }
            LinkedList<Wf> wordsL = fromSpanGetAllMentions(tarlist,vars);
            generateMIDAndSetIdWF(wordsL, m,vars);
            deg = "MentionId:" + m.getID() + deg;

	        for (FactVal tts : fvObj.getFactVal()) {
	        	if(tts.getResource().equalsIgnoreCase("factbank")){
	        		 m.add(NWR.FACT_BANK,  tts.getValue());
	        	}
	        }           
            logDebug(deg,vars);
            int addedNew = addOrMergeAMention(m,vars);
            if (addedNew==1|addedNew==0){
            	vars.factualityMentions2++;
            	vars.factualityMentions++;
            }
            String charS2 = m.getUnique(NIF.BEGIN_INDEX, Integer.class) + "," + m.getUnique(NIF.END_INDEX, Integer.class);
            vars.entityMentions.put(charS2,m);
        
      }
    }

  
    private static void getCLinksMentions(CausalRelations causalRelations,processNAFVariables vars) {
        if (!checkHeaderTextTerms(vars)) {
            logError("Error: populating interrupted",vars);
        } else {
            logDebug("Start mapping the CLINKS mentions:",vars);
        }
        for (Clink fvObj : causalRelations.getClink()) {

            String deg = "";
            Record m = Record.create();
            m.add(RDF.TYPE, NWR.CLINK,NWR.RELATION_MENTION,
                    KS.MENTION);
            deg += "|TYPE:CLINK,RELATION_MENTION,MENTION";
            m.add(KS.MENTION_OF, vars.news_file_id);
           
            
            LinkedList<Target> tarlist = new LinkedList<Target>();
            List<Target> from = getSpanTermsOfPredicate(fvObj.getFrom(),vars);
            List<Target> to = getSpanTermsOfPredicate(fvObj.getTo(),vars);
	        tarlist.addAll(from);
	        tarlist.addAll(to);	           
	        
	        LinkedList<Wf> fromwl = fromSpanGetAllMentions(from,vars);
            Record mtest1 = Record.create();
			generateMIDAndSetIdWF(fromwl, mtest1 ,vars);
	        m.add(NWR.SOURCE, mtest1.getID());
	        
	        LinkedList<Wf> towl = fromSpanGetAllMentions(to,vars);
            Record mtest2 = Record.create();
			generateMIDAndSetIdWF(towl, mtest2 ,vars);
            m.add(NWR.TARGET, mtest2.getID());
	        
            LinkedList<Wf> wordsL = fromSpanGetAllMentions(tarlist,vars);
            generateMIDAndSetIdWF(wordsL, m,vars);
            deg = "MentionId:" + m.getID() + deg;
	                 
            logDebug(deg,vars);
            int addedNew = addOrMergeAMention(m,vars);
            if (addedNew==1){
            	vars.clinkMentions++;
            }else if (addedNew==1){
            	vars.clinkMentionsDiscarded++;
            }
           
      }
    }

  
    private static void getTLinksMentions(TemporalRelations temporalRelations,processNAFVariables vars) {
        if (!checkHeaderTextTerms(vars)) {
            logError("Error: populating interrupted",vars);
        } else {
            logDebug("Start mapping the TLINKS mentions:",vars);
        }

		if (temporalRelations == null) {
			return;
		}
		if (temporalRelations.getTlink() == null) {
			return;
		}

        for (Tlink fvObj : temporalRelations.getTlink()) {

            String deg = "";
            Record m = Record.create();
            m.add(RDF.TYPE, NWR.TLINK,NWR.RELATION_MENTION,
                    KS.MENTION);
            deg += "|TYPE:TLINK,RELATION_MENTION,MENTION";
            m.add(KS.MENTION_OF, vars.news_file_id);
           
            
            LinkedList<Target> tarlist = new LinkedList<Target>();
            List<Target> from = new ArrayList<Target>();
            List<Target> to = new ArrayList<Target>();
            if(fvObj.getFromType().equalsIgnoreCase("event")){
            	from = getSpanTermsOfPredicate(fvObj.getFrom(),vars);
            }
            
            if(fvObj.getToType().equalsIgnoreCase("event")){
            	 to = getSpanTermsOfPredicate(fvObj.getTo(),vars);
            }

            tarlist.addAll(from);
	        tarlist.addAll(to);	           
	        
	        LinkedList<Wf> allEventWF = fromSpanGetAllMentions(tarlist,vars);
	        LinkedList<Wf> allWFFrom = fromSpanGetAllMentions(from,vars);
	        LinkedList<Wf> allWFTO = fromSpanGetAllMentions(to,vars);

	        List<Wf> fromtmx = new ArrayList<Wf>();
            List<Wf> totmx = new ArrayList<Wf>();
	        //here if the type is timex, get all its WFs
	        if(fvObj.getFromType().equalsIgnoreCase("timex")){
	        	fromtmx=getSpanOfTimex(fvObj.getFrom(),vars);
	        	allWFFrom.addAll(fromtmx);
	        	allEventWF.addAll(fromtmx);
            }
            
            if(fvObj.getToType().equalsIgnoreCase("timex")){
            	 totmx = getSpanOfTimex(fvObj.getTo(),vars);
            	 allWFTO.addAll(totmx);
            	 allEventWF.addAll(totmx);
            }
            
            //reorder the lists
             allEventWF = reorderWFAscending(allEventWF,vars);
	         allWFFrom = reorderWFAscending(allWFFrom,vars);
	         allWFTO = reorderWFAscending(allWFTO,vars);
            
	         if(fvObj.getFrom().equalsIgnoreCase("tmx0")){
	        	 	Record mtest2 = Record.create();
					generateMIDAndSetIdWF(allWFTO, mtest2 ,vars);
	        		int returned=addTlinkRelTypeToMention(NWR.TLINK_FROM_TMX0,vars.tLinkTypeMapper.get(fvObj.getRelType().toUpperCase()),mtest2,vars);
	        		if (returned==1)
	        		vars.tlinkMentionsEnriched++;
	        		else
	        		logDebug("Tlink FROM -> tmx0, not found the target mention id:" + fvObj.getTo(), vars);
	        		continue;
	         }
	         if(fvObj.getTo().equalsIgnoreCase("tmx0")){
	        	 	Record mtest1 = Record.create();
					generateMIDAndSetIdWF(allWFFrom, mtest1 ,vars);
					int returned=addTlinkRelTypeToMention(NWR.TLINK_TO_TMX0,vars.tLinkTypeMapper.get(fvObj.getRelType().toUpperCase()),mtest1,vars);
					if (returned==1)
					vars.tlinkMentionsEnriched++;
					else
		        		logDebug("Tlink TO -> tmx0, not found the source mention id:" + fvObj.getFrom(), vars);
					continue;
	        	}
             if(allWFFrom.size()>0){
	            Record mtest1 = Record.create();
				generateMIDAndSetIdWF(allWFFrom, mtest1 ,vars);
		        m.add(NWR.SOURCE, mtest1.getID());
             }
             if(allWFTO.size()>0){
	            Record mtest2 = Record.create();
				generateMIDAndSetIdWF(allWFTO, mtest2 ,vars);
	            m.add(NWR.TARGET, mtest2.getID());
             }
            m.add(NWR.REL_TYPE, vars.tLinkTypeMapper.get(fvObj.getRelType().toUpperCase()));
            generateMIDAndSetIdWF(allEventWF, m,vars);
            deg = "MentionId:" + m.getID() + deg;
	                 
            logDebug(deg,vars);
            int addedNew = addOrMergeAMention(m,vars);
            if (addedNew==1){
            	vars.tlinkMentions++;
            }else if (addedNew==-1){
            	vars.tlinkMentionsDiscarded++;
            }
           
      }
    }

  
    private static int addTlinkRelTypeToMention(URI key, URI value,
			Record mention, processNAFVariables vars) {
        String charS = mention.getUnique(NIF.BEGIN_INDEX, Integer.class) + "," + mention.getUnique(NIF.END_INDEX, Integer.class);
    	if(vars.mentionListHash.containsKey(charS)){
    		vars.mentionListHash.get(charS).add(key, value);
    		return 1;
    	}
    	return -1;
	}

	private static LinkedList<Wf> reorderWFAscending(LinkedList<Wf> list,
			processNAFVariables vars) {
    	LinkedList<Wf> tmp = new LinkedList<Wf>();
        int found = 0;
                for (Wf wftmp : vars.doc.getText().getWf()) {
                    if (list.contains(wftmp)) {
                        tmp.addLast(wftmp);
                        found++;
                    }
                    if (found >= list.size()) {
                        break;
                    }

                }
        
        if (found < list.size()) {
			logDebug("reorderWFAscending method, inconsistency: returned list less than the input list", vars);
        }
    return tmp;
	}

	private static List<Wf> getSpanOfTimex(String tmxId, processNAFVariables vars) {
    	List<Wf> tmp = new ArrayList<Wf>();
		for(Timex3 tms:vars.doc.getTimeExpressions().getTimex3()){
			if(tms.getId().equalsIgnoreCase(tmxId)){
				for(Target t: tms.getSpan().getTarget()){
					tmp.add((Wf) t.getId());
				}
				break;
			}
		}
		return tmp;
	}

	private static List<Target> getSpanTermsOfPredicate(String predicateId,
			processNAFVariables vars) {
		for( Predicate pr:vars.doc.getSrl().getPredicate()){
			if(pr.getId().equalsIgnoreCase(predicateId)){
				return pr.getSpan().getTarget();
			}
		}
		return null;
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

    private static void getSRLMentions(processNAFVariables vars) {
        Srl obj = vars.doc.getSrl();
        if (!checkHeaderTextTerms(vars)) {
            logError("Error: populating interrupted",vars);
        } else {
            logDebug("Start mapping the Srl mentions:",vars);
        }

	if ((obj == null) || (((Srl) obj).getPredicate() == null)) {
            logError("skipped missing xpath(//NAF/srl)",vars);
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
                    logDebug("Srl should have one span only! ", vars);
                }
                if (!firstSpanFound) {
                    firstSpanFound = true;
                }
		predicateCharSpan = getCharSpanFromSpan(prdObj.getSpan(),vars);
                mtmp = Record.create();
                mtmp.add(RDF.TYPE, NWR.EVENT_MENTION, NWR.TIME_OR_EVENT_MENTION,
                        NWR.ENTITY_MENTION, KS.MENTION);
                deg = "|TYPE:EVENT_MENTION,TIME_OR_EVENT_MENTION,ENTITY_MENTION,MENTION";
		mtmp.add(KS.MENTION_OF, vars.news_file_id);
                for (Target tars : prdObj.getSpan().getTarget()) {
                    tars.getId();
                    Term eventTerm = getTermfromTermId((Term) tars.getId(),vars);
                    eventTermList.addLast(eventTerm);
                    if (eventTerm.getLemma() != null) {
                        mtmp.add(NWR.PRED, eventTerm.getLemma());
                        deg += "|PRED:" + eventTerm.getLemma();
                    }
                    if (eventTerm.getPos() != null) {
			URI posVal = (eventTerm.getPos().equals("V") || 
				      eventTerm.getPos().equals("N")) 
			    ? vars.partOfSpeechMapper.get(eventTerm.getPos())
			    : vars.partOfSpeechMapper.get("");
                        mtmp.add(NWR.POS, posVal);
                        deg += "|POS:" + posVal;
                    }
                }
                generateTheMIdAndSetID(prdObj.getSpan(), mtmp,vars);
                deg = "MentionId:" + mtmp.getID() + deg;

            }
            
            /*
             * Assign the predicateAnchor attributes to the predicate Mention
             */
            
           List<PredicateAnchor> prds= getAllRelativePredicateAnchors(prdObj.getId(),vars);
           for(PredicateAnchor tprd: prds){
        	   
        	   if(tprd.getAnchorTime()!=null){
        	    LinkedList<Wf> wfL = reorderWFAscending(fromSpanGetAllMentionsTmx(((Timex3)tprd.getAnchorTime()).getSpan().getTarget(), vars),vars);
                if(wfL.size()>0){
        	    Record mtest1 = Record.create();
       			generateMIDAndSetIdWF(wfL, mtest1 ,vars);       	        
        		mtmp.add(NWR.ANCHOR_TIME, mtest1.getID());
                }
        	   }
        	   
        	   if(tprd.getBeginPoint()!=null){
        		   LinkedList<Wf> wfL = reorderWFAscending(fromSpanGetAllMentionsTmx(((Timex3)tprd.getBeginPoint()).getSpan().getTarget(), vars),vars);
                   if(wfL.size()>0){
        		   Record mtest1 = Record.create();
        		   generateMIDAndSetIdWF(wfL, mtest1 ,vars);
            	   mtmp.add(NWR.BEGIN_POINT, mtest1.getID());
                   }
        	   }
        	   
        	   if(tprd.getEndPoint()!=null){
        		   LinkedList<Wf> wfL = reorderWFAscending(fromSpanGetAllMentionsTmx(((Timex3)tprd.getEndPoint()).getSpan().getTarget(), vars),vars);
                   if(wfL.size()>0){
        		   Record mtest1 = Record.create();
        		   generateMIDAndSetIdWF(wfL, mtest1 ,vars);
            	   mtmp.add(NWR.END_POINT, mtest1.getID());
                   }
        	   }
           }
           deg +="| ANCHOR_TIME:"+mtmp.get(NWR.ANCHOR_TIME)+" | BEGIN_POINT:"+mtmp.get(NWR.BEGIN_POINT)+" | END_POINT:"+mtmp.get(NWR.END_POINT);
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
                        logDebug("more than one external ref for predicate:" + predicateID
								+ " size: " + predicatExtRef, vars);
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

				    URI resourceMappedValue = vars.srlExternalRefResourceTypeMapper.get(resourceValue);
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
					referenceMappedValue = vars.eventClassMapper.get(referenceValue);
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
					logDebug("xpath(//NAF/srl/predicate/externalReferences/externalRef@Resource(NULL)): predicateID("
							+ predicateID + ")", vars);
                            }
                        } else {
							logDebug("Mapping error - Mention null - xpath(NAF/srl/predicate/externalReferences/externalRef): predicateID("
									+ predicateID + ")", vars);
                        }
                    }
                    if (!eventTypeFound) {
                        mtmp.add(NWR.EVENT_CLASS, vars.eventClassMapper.get("contextual"));
                        deg += "|EVENT_CLASS:" + vars.eventClassMapper.get("contextual");
                        eventTypeFound = true;
                    }
                    if (eventTypeFound) {
                        logDebug(deg,vars);
                        int addedNew = addOrMergeAMention(mtmp,vars);
                        
			logDebug("ROL1: <srl> <predicate> adding new event mention for id " 
				+ predicateID + ", charSpan |" + predicateCharSpan + "|",vars);
			
                        if (addedNew==1|addedNew==0){
                        	vars.srlMention2++;
                        	vars.srlMention++;
                        }
                        String charS2 = mtmp.getUnique(NIF.BEGIN_INDEX, Integer.class) + "," + mtmp.getUnique(NIF.END_INDEX, Integer.class);
                        vars.entityMentions.put(charS2,mtmp);
                        
                        eventMentionId = mtmp.getID().stringValue();
                    } else {
                        // if eventType not found or no mapping for it
                        // write error and discard this mention
                        logDebug("Mention discarded for predicateID(" + predicateID + ") - ID("
								+ mtmp.getID().toString() + ")", vars);
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
			String roleCharSpan = getCharSpanFromSpan(roleSpan,vars);
			boolean createTemporalexpressionMentionFlag = false;
			boolean createTlinkFlag = false;
			boolean createParticipationMentionFlag = false;

			String semRole = ((Role) prdGObj).getSemRole();
			if ((semRole != null) && (semRole.equalsIgnoreCase("AM-TMP"))) {
			    createTlinkFlag = true;
			    createTemporalexpressionMentionFlag = true;
			    logDebug(" ROL1: <srl> <role> found TLINK for |" + predicateCharSpan + "|" + roleCharSpan + "|",vars);
			} else {
			    if (checkAlreadyAcceptedMention(roleCharSpan,vars)) {
				createParticipationMentionFlag = true;
				logDebug(" ROL1: <srl> <role> found already existent mention for |" + roleCharSpan + "|",vars);
			    }
			}


			/*
			  create a temporal-expression mention with span <role><span> if not already extracted
			*/
			if (createTemporalexpressionMentionFlag) {
			    if (! checkAlreadyAcceptedMention(roleCharSpan,vars)) {
				Record roleM = Record.create();
				roleM.add(RDF.TYPE, NWR.TIME_MENTION, NWR.TIME_OR_EVENT_MENTION, 
					  NWR.ENTITY_MENTION, KS.MENTION);
				roleM.add(KS.MENTION_OF, vars.news_file_id);

				//  compute the Term list for the <role>
				roleTermList = new LinkedList<Term>();
				for (Target rspnTar : ((Role) prdGObj).getSpan().getTarget()) {
				    Term ttmp = getTermfromTermId((Term) rspnTar.getId(),vars);
				    roleTermList.addLast(ttmp);
				}
			    
				// generate and set ID of the mention
				generateTheMIdAndSetID(roleSpan, roleM,vars);

				// try to add the mention
				if(addOrMergeAMention(roleM,vars) == 1) {
					vars.timeMention2++;
				}

				logDebug("   ROL1: created temporal-expression mention for |" + roleCharSpan + "|",vars);
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
					logDebug("//NAF/srl/predicate/role/ - a Role without Predicate roleID("
							+ ((Role) prdGObj).getId() + ")", vars);
			    }

			    /*
			      set as NWR.TARGET of the relation mention the id of the role mention related to <role>
			    */
			    {
				URI roleId = getMentionIDFromCharSpan(roleCharSpan,vars);
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
			    relationM.add(KS.MENTION_OF, vars.news_file_id);

			    /* 
			       generate and set ID of the relation mention
			    */
			    //  compute the Term list for the <role>
			    if (roleTermList == null) {
				roleTermList = new LinkedList<Term>();
				for (Target rspnTar : ((Role) prdGObj).getSpan().getTarget()) {
				    Term ttmp = getTermfromTermId((Term) rspnTar.getId(),vars);
				    roleTermList.addLast(ttmp);
				}
			    }
			    generateTheMIdAndSetID_forParticipationMention(eventTermList, roleTermList, relationM,vars);
			    deg2 = "MentionId:" + relationM.getID() + deg2;
			    boolean create = false;

			    /* 
			       always add the relation mention (do not check if a previously accepted mention with the span of <role> exists)
			    */
			    int addedNew = -1;
			    MenCreated = true;

			    charS = relationM.getUnique(NIF.BEGIN_INDEX, Integer.class) + "," + relationM.getUnique(NIF.END_INDEX, Integer.class);
			    if (createParticipationMentionFlag) {
				logDebug("  ROL1: <srl> <predicate> <role> adding new participation mention for |" + charS + "|",vars);
			    } else {
				logDebug("  ROL1: <srl> <predicate> <role> adding new TLINK mention for |" + charS + "|",vars);
			    }

			    /* 
			       try to add the relation mention
			    */
			    addedNew = addOrMergeAMention(relationM,vars);
			    if (addedNew==1){
			    	vars.rolewithEntity++;
			    	vars.rolewithEntity2++;
			    	vars.roleMentions++;
			    } else {
				// if with entity and conflict or enriched, counted as discarded
				if(create){
					vars.rolewithoutEntity++;
				}
			    }

			    /* 
			       add the information from <externalReferences> sub-tag (the relation mention is always created)
			    */
			    for (ExternalReferences roleGOBJ : ((Role) prdGObj).getExternalReferences()) {
				    for (ExternalRef rexRefObj : roleGOBJ.getExternalRef()) {

					// check 'resource' and 'reference' attributes
					//
					String resourceValue = rexRefObj.getResource();
					String referenceValue = rexRefObj.getReference();

					if (resourceValue != null) {
					    
					    URI resourceMappedValue = vars.srlExternalRefResourceTypeMapper.get(resourceValue);
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
					    	vars.mentionListHash.get(charS).add(resourceMappedValue, valueURI);
					    }
					    deg2 += "|" + resourceMappedValue + ":" + valueURI;
					}  else {
					    // resourceValue == null)
						logDebug("xpath(//NAF/srl/predicate/role/externalReferences/externalRef@Resource(NULL)): RoleID("
								+ ((Role) prdGObj).getId() + ")", vars);
					}
				    }
				
			    } // end of adding info from <externalReferences> within <role>
			}
			logDebug(deg2,vars);
			
		    } // end of <role> processing

            } // end of iteration over <externalReferences> or <role>s of the <predicate>

        } // end of iteration over <predicate>
    }


    private static List<PredicateAnchor> getAllRelativePredicateAnchors(String id,
			processNAFVariables vars) {
    	List<PredicateAnchor> tmp= new ArrayList<PredicateAnchor>();
    	for( PredicateAnchor pas:vars.doc.getTemporalRelations().getPredicateAnchor()){
    		for(Span t: pas.getSpan()){
    			for(Target tm: t.getTarget()){
    				if(((Predicate)tm.getId()).getId().equalsIgnoreCase(id)){
    					tmp.add(pas);
    					break;
    				}
    			}
    		}
    	}
		return tmp;
	}

	
	private static void getNAFHEADERMentions(NafHeader obj,processNAFVariables vars) {
        logDebug("Start reading the naf metadata:",vars);
        String deg = "";
        Public publicProp = ((NafHeader) obj).getPublic();
        initURIIDS(publicProp,vars);
        Record newsFile = Record.create();
        Record nafFile = Record.create();
        vars.nafFile2 = nafFile;
        vars.newsFile2 = newsFile;
        newsFile.setID(vars.news_file_id);
        nafFile.setID(vars.NAF_file_id);
        deg += "news_file_id:" + vars.news_file_id;
        newsFile.add(RDF.TYPE, NWR.NEWS);
        deg += "\nNAF_file_id:" + vars.NAF_file_id;

        nafFile.add(RDF.TYPE, NWR.NAFDOCUMENT);
        nafFile.add(NWR.ANNOTATION_OF, newsFile.getID());
        newsFile.add(NWR.ANNOTATED_WITH, nafFile.getID());
        if (vars.doc.getVersion() != null) {
            nafFile.add(NWR.VERSION, vars.doc.getVersion());
            deg += "\nVERSION:" + vars.doc.getVersion();
        }

	/* 
	   set DCTERMS.SOURCE according to the News URI
	*/
	URIImpl sourceURL;
	if (vars.PREFIX.matches("(?i)http://www.newsreader-project.eu/LNdata.*")) {
	    // LexisNexis news
	    String preStr = "http://www.lexisnexis.com/uk/nexis/docview/getDocForCuiReq?lni=";
	    String postStr = "&csi=138620&perma=true";
	    String srcUrlstr = new String(preStr + publicProp.getPublicId() + postStr);
	    sourceURL = new URIImpl(srcUrlstr);
	} else {
	    // non-LexisNexis news
	    sourceURL = (URIImpl)vars.news_file_id;
	}
	newsFile.add(DCTERMS.SOURCE, sourceURL);
	
	if (publicProp.getPublicId() != null) {
            nafFile.add(DCTERMS.IDENTIFIER, publicProp.getPublicId());// NAF/nafHeader/public@publicId
            deg += "|IDENTIFIER:" + publicProp.getPublicId();
        }
        if (vars.doc.getXmlLang() != null) {
            newsFile.add(DCTERMS.LANGUAGE, Data.languageCodeToURI(vars.doc.getXmlLang()));
            deg += "|LANGUAGE:" + Data.languageCodeToURI(vars.doc.getXmlLang());
        } else {
            logWarn("Language not catched:" + vars.doc.getXmlLang(),vars);
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
            
            
            if (fileDesc.getSection() != null) {
                newsFile.add(NWR.SECTION, fileDesc.getSection());
                deg += "|SECTION:" + fileDesc.getSection();
            }
           
            if (fileDesc.getMagazine() != null) {
                newsFile.add(NWR.MAGAZINE, fileDesc.getMagazine());
                deg += "|MAGAZINE:" + fileDesc.getMagazine();
            }
            
            if (fileDesc.getLocation() != null) {
                newsFile.add(NWR.LOCATION, fileDesc.getLocation());
                deg += "|LOCATION:" + fileDesc.getLocation();
            }
            
            if (fileDesc.getPublisher() != null) {
                newsFile.add(NWR.PUBLISHER, fileDesc.getPublisher());
                deg += "|PUBLISHER:" + fileDesc.getPublisher();
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
            logWarn("FileDesc: null",vars);
        }
        for (LinguisticProcessors lpObj : ((NafHeader) obj).getLinguisticProcessors()) {
            deg += "\n";
            if (lpObj.getLayer() != null) {
                if (vars.nafLayerMapper.containsKey(lpObj.getLayer())
                        && vars.nafLayerMapper.get(lpObj.getLayer()) != null) {
                    nafFile.add(NWR.LAYER, vars.nafLayerMapper.get(lpObj.getLayer()));
                    deg += "LAYER:" + vars.nafLayerMapper.get(lpObj.getLayer());
                } else {
					logDebug("xpath(//NAF/nafHeader/linguisticProcessors/@layer["
							+ lpObj.getLayer() + "]),  unknown layer.", vars);
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
                String uri = vars.PREFIX + (vars.PREFIX.endsWith("/") ? "" : "/") + "lp/" + namuri + "/"
                        + lpO.getVersion();

                URI rId = new URIImpl(uri);
                r3.setID(rId);
                nafFile.add(NWR.MODULES, r3);
                deg += "|CREATOR:" + r3;
            }
        }

        logDebug(deg,vars);
    }


    private static void getCoreferencesMentions(Coreferences obj,processNAFVariables vars) {
        if (!checkHeaderTextTerms(vars)) {
            logError("Error: populating interrupted",vars);
        } else {
            logDebug("Start mapping the Coreferences mentions:",vars);
        }
        String deg = "\n";
	
	/*
	  process the <coref> tag
	*/
        for (Coref corefObj : ((Coreferences) obj).getCoref()) {
            deg = "";
            if (corefObj.getSpan().size() < 1) {
                logDebug("Every coref must contain a 'span' element inside 'references'", vars);
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
		    String corefCharSpan = getCharSpanFromSpan(corefSpan,vars);
		    Object[] retArray = checkSpanIncludesAnAlreadyAcceptedMention(corefCharSpan,vars);
		    int inclFlag = ((Integer) retArray[0]).intValue();
		    if ((inclFlag == 1) || (inclFlag == 2)) {
			addMentionsFlag = true;
			String includedMentionCharSpan = (String)retArray[1];
			typesOfIncludedMention = getMentionTypeFromCharSpan(includedMentionCharSpan,vars);
			logDebug("ROL1: <coref> id " + corefObj.getId() + ": found included mention for |" 
				 + corefCharSpan + "|, included mention |" + includedMentionCharSpan 
				 + "|, inclFlag " + inclFlag
				 + ", types " + getTypeasString(typesOfIncludedMention),vars);
			break;
		    }
		}
	    }

	    if (addMentionsFlag) {
		for (Span corefSpan : corefObj.getSpan()) {
		    String corefCharSpan = getCharSpanFromSpan(corefSpan,vars);
		    if (checkAlreadyAcceptedMention(corefCharSpan,vars)) {
			logDebug("ROL1: <coref> id " + corefObj.getId() + ": skipping already existent mention with charSpan |" 
				 + corefCharSpan + "|",vars);
			continue;
		    }
		    deg = "";
		    Record m = Record.create();
		    m.add(KS.MENTION_OF, vars.news_file_id);
		    deg += "MENTION_OF:" + vars.news_file_id;
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
			    + corefCharSpan + "|, and type " + m.get(RDF.TYPE),vars);

		    if (corefSpan.getTarget().size() < 1) {
			logDebug("Every span in an entity must contain at least one target inside", vars);
		    }
		    for (Target spTar : corefSpan.getTarget()) {
			if (eventM) {
			    Term eventTerm = getTermfromTermId((Term) spTar.getId(),vars);
			    m.add(NWR.PRED, eventTerm.getLemma());
			    deg += "|PRED:" + eventTerm.getLemma();
			    if (eventTerm.getPos() != null) {
				URI posVal = (eventTerm.getPos().equals("V") || 
					      eventTerm.getPos().equals("N")) 
				    ? vars.partOfSpeechMapper.get(eventTerm.getPos())
				    : vars.partOfSpeechMapper.get("");
				m.add(NWR.POS, posVal);
				deg += "|POS:" + posVal;
			    } else {
					logDebug("//NAF/coreferences/coref/span/target/@id/@getPOS[null], id("
							+ eventTerm.getId() + ")", vars);
			    }
			}
			if (!eventM && spTar.getHead() != null && spTar.getHead().equals("yes")) {
			    if (spTar.getId() != null) {
				m.add(NWR.SYNTACTIC_HEAD, spTar.getId());
				deg += "|SYNTACTIC_HEAD:" + spTar.getId();
			    } else {
					logDebug("//NAF/coreferences/coref/span/target[@head='yes']/@id[null], id("
							+ spTar.getId() + ")", vars);
			    }
			}
		    }
		    generateTheMIdAndSetID(corefSpan, m,vars);
		    deg = "MentionId:" + m.getID() + deg;
		    logDebug(deg,vars);

		    int addedNew = addOrMergeAMention(m,vars);
                
		    if (!eventM){
			// eventM == false
			if(addedNew==1){
			    vars.corefMention2++;
			    vars.no_mapping++;
			    vars.corefMentionNotEvent++;
			    vars.corefMention++;
			    // logWarn("xpath(//NAF/coreferences/coref/) add a new object mention, missing type.");
			}
		    } else {
			// eventM == true
			if(addedNew==1){
				vars.srlMention2++;
				vars.corefMentionEvent++;
				vars.corefMention++;
			}
		    }
            String charS2 = m.getUnique(NIF.BEGIN_INDEX, Integer.class) + "," + m.getUnique(NIF.END_INDEX, Integer.class);
		    vars.entityMentions.put(charS2,m);
		}
	    } else {
		logDebug("ROL1: <coref> id " + corefObj.getId() + ": entirely skipped, NO included mentions",vars);
	    }
        }
    }


    private static LinkedList<Term> mergeTwoTermLists(LinkedList<Term> eventTermList,
            LinkedList<Term> roleTermList,processNAFVariables vars) {

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
		 + merged.size() + ").",vars);
	return merged;
    }

    // given a charSpan (e.g. "321,325") check if there is an already accepted mention with such span
    //
    private static boolean checkAlreadyAcceptedMention(String charSpan,processNAFVariables vars) {
	return vars.mentionListHash.containsKey(charSpan);
    }


    // given a charSpan (e.g. "321,325") check if it includes an already accepted mention; 
    // return an array of 3 Objects: (Integer)inclFlag, (String)outCharSpan where
    //   0, null                    if there are no already-accepted mentions included in the charSpan
    //   1, charSpan                if an already-accepted mention coincides with the charSpan
    //   2, chSpanOfIncludedMention if an already-accepted mention is stricly included in the charSpan
    //
    private static Object[] checkSpanIncludesAnAlreadyAcceptedMention(String charSpan,processNAFVariables vars) {
	Object[] retArray = new Object[2];
	// check if an already-accepted mention coincides
	if (checkAlreadyAcceptedMention(charSpan,vars)) {
	    retArray[0] = new Integer(1);
	    retArray[1] = charSpan;
	    return retArray;
	}

	// check if an already-accepted mention is strictly included
	String[] fields = charSpan.split(",");
	int spanBeginC = Integer.parseInt(fields[0]);
	int spanEndC   = Integer.parseInt(fields[1]);

	Enumeration keys = vars.mentionListHash.keys();
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
    private static String getCharSpanFromSpan(Span sp,processNAFVariables vars) {
        LinkedList<Wf> wordsL = fromSpanGetAllMentions(sp.getTarget(),vars);
        String begin = wordsL.getFirst().getOffset();
	Wf lastW = wordsL.getLast();
        int end = Integer.parseInt(lastW.getOffset()) + Integer.parseInt(lastW.getLength());
	String charSpan = begin + "," + Integer.toString(end);
	return charSpan;
    }

    // given a charSpan (e.g. "321,325") get the ID of the mention with such span if exists, otherwise return null
    //
    private static URI getMentionIDFromCharSpan(String charSpan,processNAFVariables vars) {
	if (vars.mentionListHash.containsKey(charSpan)) {
	    return vars.mentionListHash.get(charSpan).getID();
	} else {
	    return null;
	}
    }

    // given a charSpan (e.g. "321,325") get the type of the mention with such span if exists, otherwise return null
    //
    private static List<Object> getMentionTypeFromCharSpan(String charSpan,processNAFVariables vars) {
	if (vars.mentionListHash.containsKey(charSpan)) {
	    return vars.mentionListHash.get(charSpan).get(RDF.TYPE);
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
    private static Integer addOrMergeAMention(Record m,processNAFVariables vars) {
        String charS = m.getUnique(NIF.BEGIN_INDEX, Integer.class) + "," + m.getUnique(NIF.END_INDEX, Integer.class);
        if (vars.mentionListHash.containsKey(charS)) {
	    /* 
	       there is a previously accepted mention with the same span: try to enrich it
	    */
            boolean chk = checkClassCompatibility(vars.mentionListHash.get(charS), m);
            if (!chk) {
		/* 
		   there is conflict between the input mention and the previously accepted mention with the same span:
		   check if the new mention can replace the old one, otherwise report the error
		*/
		if (checkMentionReplaceability(vars.mentionListHash.get(charS), m)){
		    // replace the old mention with the new one
			vars.mentionListHash.put(charS, m);
		    logDebug("Replacement with Mention: " + m.getID() + ", class(" + getTypeasString(m.get(RDF.TYPE)) + ")", vars);
		    return 0;
		}

                String types =getTypeasString(m.get(RDF.TYPE));
                if(types.contains(NWR.PARTICIPATION.stringValue())){
                    logDebug("Participation collision error, mentionID(" + m.getID() + ") class1(" + getTypeasString(m.get(RDF.TYPE)) + "), class-pre-xtracted(" + getTypeasString(vars.mentionListHash.get(charS).get(RDF.TYPE)) + ")", vars);
                }else{
                    logDebug("Generic collision error, mentionID(" + m.getID() + ") class1(" + getTypeasString(m.get(RDF.TYPE)) + "), class-pre-xtracted(" + getTypeasString(vars.mentionListHash.get(charS).get(RDF.TYPE)) + ")", vars);
                }
                return -1;
            } else {
		/* 
		   there is compatibility between the input mention and the previously accepted mention with the same span:
		   enrich old mention with properties from the new one (except participation mentions)
		*/
                String types =getTypeasString(m.get(RDF.TYPE));
                if(types.contains(NWR.PARTICIPATION.stringValue())){//Rule: no enrichment for participation
                    logDebug("Refused enrichment with participation mention, mentionID(" + m.getID() + ")", vars);
                    return -1;
                }
		// enrich mention
		ListIterator<URI> mit = m.getProperties().listIterator();
		while (mit.hasNext()) {
		    URI mittmp = mit.next();
		    
		    for (Object pit : m.get(mittmp)) {
		    	vars.mentionListHash.get(charS).add(mittmp, pit);
		    }
		}
		logDebug("Mention enrichment: " + m.getID() + ", class(" + getTypeasString(m.get(RDF.TYPE)) + ")", vars);
		return 0;
            }
            
        } else {
	    /* 
	       the mention is new (there is no previously accepted mention with the same span)
	    */
        	vars.mentionListHash.put(charS, m);
	    logDebug("Created Mention: " + m.getID(),vars);
	    return 1;
	}
    }

    // apply to all the mentions the following changes:
    //  - add the "extent" attribute as NIF.ANCHOR_OF
    //
    private static void fixMentions(processNAFVariables vars) {
	Enumeration keys = vars.mentionListHash.keys();
	while( keys.hasMoreElements() ) {
	    String key = (String) keys.nextElement();
	    Record m = (Record) vars.mentionListHash.get(key);

	    // get charStartIndex and charEndIndex from the mention charSpan (= the key)
	    //
	    String[] csList = key.split(",");
	    int cStart = Integer.parseInt(csList[0]);
	    int cEnd = Integer.parseInt(csList[1]);
	    String extentStr = vars.rawText.substring(cStart, cEnd);
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
	boolean isGenericOldM = (typesOld.contains(NWR.OBJECT_MENTION) && (oldM.get(NWR.ENTITY_TYPE) == null||oldM.get(NWR.ENTITY_TYPE).size() == 0));
	boolean isSpecificNewM = ((typesNew.contains(NWR.OBJECT_MENTION) && (newM.get(NWR.ENTITY_TYPE) != null && oldM.get(NWR.ENTITY_TYPE).size() > 0))
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

    private static void initURIIDS(Public publicProp,processNAFVariables vars) {
        if (publicProp.getPublicId() == null) {
            logError("Corrupted Naf file: PublicId in the Naf header is missed",vars);
            System.exit(0);
        }
        vars.nafPublicId = publicProp.getPublicId();
        String uri = publicProp.getUri();
        //TODO remove it @mohammed Sept2014PREFIX
        //uri = PREFIX+uri;
        vars.news_file_id = new URIImpl(uri);
        String nafuri = uri + ".naf";
        vars.NAF_file_id = new URIImpl(nafuri);
        logDebug("news_file_id: " + uri,vars);
        logDebug("NAF_file_id: " + nafuri,vars);

	// set the PREFIX given the news uri
	try {
	    URL nurl = new URL(uri);
	    Path p = Paths.get(nurl.getPath());
	    vars.PREFIX = nurl.getProtocol() + "://" + nurl.getAuthority() + "/" + p.subpath(0,2);
	} catch (Exception me) {
		vars.PREFIX = vars.news_file_id.getNamespace();
	}
        logDebug("PREFIX: " + vars.PREFIX,vars);
    }

    static void generateMIDAndSetIdWF(LinkedList<Wf> wordsL, Record m,processNAFVariables vars) {
        int begin = Integer.parseInt(wordsL.getFirst().getOffset());
        int end = (Integer.parseInt(wordsL.getLast().getOffset()) + Integer.parseInt(wordsL
                .getLast().getLength()));
        m.add(NIF.BEGIN_INDEX, begin);
        m.add(NIF.END_INDEX, end);
        String tmpid = vars.news_file_id + "#char=" + begin + "," + end;
        URI mId = new URIImpl(tmpid);
        m.setID(mId);
    }

    private static void logError(String error,processNAFVariables vars) {
        if (vars.logErrorActive) {
        	vars.logger.error(vars.filePath.getName() + " " + error);
        }
        if (!vars.storePartialInforInCaseOfError) {
            System.exit(-1);
        }

    }

    private static void logDebug(String error,processNAFVariables vars) {
        if (vars.logDebugActive) {
        	vars.logger.debug(error);
        }
    }
    private static void logWarn(String error,processNAFVariables vars) {
    	vars.logger.warn(vars.filePath.getName() + " "+error);
    }

    private static boolean checkHeaderTextTerms(processNAFVariables vars) {
        if (vars.globalTerms == null) {
            logWarn("Error: No term(s) has been catched!",vars);
            return false;
        }
        if (vars.globalText == null) {
            logWarn("Error: No text(s) has been catched!",vars);
            return false;
        }
        return true;
    }

    public static Logger getLogger(processNAFVariables vars) {
        return vars.logger;
    }

    public static void setLogger(final Logger logger,processNAFVariables vars) {
    	vars.logger = logger;
    }

    private static void generateTheMIdAndSetID(Span spansObj, Record m,processNAFVariables vars) {
        LinkedList<Wf> wordsL = fromSpanGetAllMentions(((Span) spansObj).getTarget(),vars);
        int begin = Integer.parseInt(wordsL.getFirst().getOffset());
        int end = (Integer.parseInt(wordsL.getLast().getOffset()) + Integer.parseInt(wordsL
                .getLast().getLength()));
        m.add(NIF.BEGIN_INDEX, begin);
        m.add(NIF.END_INDEX, end);
        String muri = vars.news_file_id + "#char=" + begin + "," + end;
        URI mId = new URIImpl(muri);
        m.setID(mId);
    }

    /* 
       similar to generateTheMIdAndSetID() but specific for ParticipationMention
     */
    private static void generateTheMIdAndSetID_forParticipationMention(LinkedList<Term> eventTermList, 
								       LinkedList<Term> roleTermList, 
								       Record m,processNAFVariables vars) {
	LinkedList<Wf> eventWordList = getTheWFListByThereTermsFromTargetList(eventTermList,vars);
	LinkedList<Wf> roleWordList = getTheWFListByThereTermsFromTargetList(roleTermList,vars);

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
	
        String muri = vars.news_file_id + "#char=" + beginIndex + "," + endIndex;
        URI mId = new URIImpl(muri);
        m.setID(mId);
    }


    /* 
       similar to generateTheMIdAndSetID() but specific for ParticipationMention
     */
    private static String getExtentOfParticipationMention(LinkedList<Term> eventTermList, 
							  LinkedList<Term> roleTermList,processNAFVariables vars) {
	LinkedList<Wf> eventWordList = getTheWFListByThereTermsFromTargetList(eventTermList,vars);
	LinkedList<Wf> roleWordList = getTheWFListByThereTermsFromTargetList(roleTermList,vars);

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
    private static boolean checkDuplicate(String muri,processNAFVariables vars) {
        boolean re = false;        
       for(String keys:vars.entityMentions.keySet()){
            if (keys.equals(muri)) {
            //if (mtmp.getID().stringValue().equals(muri)) {
                re = true;
                break;
            }
        }
        return re;
    }

    private static Term getTermfromTermId(Term termId,processNAFVariables vars) {
        if (vars.globalTerms != null) {
            if (vars.globalTerms.getTerm().contains(termId))
                return vars.globalTerms.getTerm().get(vars.globalTerms.getTerm().indexOf(termId));
        } else {
        	Terms ltmp = vars.doc.getTerms();
            if (((Terms) ltmp).getTerm().contains(termId))
            return vars.globalTerms.getTerm().get(vars.globalTerms.getTerm().indexOf(termId));
           
        }
        logWarn("Term is not found, searched TermId(" + termId.getId() + ")",vars);
        return null;
    }

    private static LinkedList<Wf> fromSpanGetAllMentionsTmx(List<Target> list,processNAFVariables vars) {
        LinkedList<Wf> returned = new LinkedList<Wf>();
        LinkedList<Wf> wordsIDL = new LinkedList<Wf>();
        for (Target ltmp : list) {
            wordsIDL.addLast((Wf) ltmp.getId());
        }
        if (vars.globalText != null) {

            int found = 0;
            for (Wf wftmp : vars.globalText.getWf()) {
                if (wordsIDL.contains(wftmp)) {
                    returned.addLast(wftmp);
                    found++;
                }
                if (found >= wordsIDL.size()) {
                    break;
                }
            }
        } else {
        	Text prop = vars.doc.getText();
                    int found = 0;
                    for (Wf wftmp : prop.getWf()) {
                        if (wordsIDL.contains(wftmp)) {
                            returned.addLast(wftmp);
                            found++;
                        }
                        if (found >= wordsIDL.size()) {
                            break;
                        }
                    }
                
            
        }
        return returned;
    }

   

    private static LinkedList<Wf> getTheWFListByThereTermsFromTargetList(LinkedList<Term> targetTermList,processNAFVariables vars) {
        LinkedList<Wf> returned = new LinkedList<Wf>();
        LinkedList<Wf> wordsIDL = new LinkedList<Wf>();
        boolean spanTermFound = false;
        if (vars.globalTerms != null) {

            for (Term termtmp : vars.globalTerms.getTerm()) {
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
        	Terms prop = vars.doc.getTerms();
                    
                    for (Term termtmp : prop.getTerm()) {
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
      /*  if (!spanTermFound) {
            logWarn("Inconsistence NAF file(#TS): Every term must contain a span element",vars);
        }*/
        if (vars.globalText != null) {

            int found = 0;
            for (Wf wftmp : vars.globalText.getWf()) {
                if (wordsIDL.contains(wftmp)) {
                    returned.addLast(wftmp);
                    found++;
                }
                if (found >= wordsIDL.size()) {
                    break;
                }
            }

            if (found < wordsIDL.size()) {
                logWarn("Inconsistence NAF file(#SW): Wf(s)  arenot found when loading term ",vars);
            }

        } else {
        	Text prop = vars.doc.getText();
            int found = 0;
                    for (Wf wftmp : prop.getWf()) {
                        if (wordsIDL.contains(wftmp)) {
                            returned.addLast(wftmp);
                            found++;
                        }
                        if (found >= wordsIDL.size()) {
                            break;
                        }

                    }
                
            
            if (found < wordsIDL.size()) {
                logWarn("Inconsistence NAF file(#SW): Wf(s)  arenot found when loading term ",vars);
            }

        }
        return returned;
    }

    private static LinkedList<Wf> fromSpanGetAllMentions(List<Target> list,processNAFVariables vars) {

        LinkedList<Term> targetTermList = new LinkedList<Term>();
        Iterator<Target> targetList = list.iterator();
        while (targetList.hasNext()) {
            Target tarm = targetList.next();
            targetTermList.add((Term)tarm.getId());
        }
        LinkedList<Wf> corrispondingWf = getTheWFListByThereTermsFromTargetList(targetTermList,vars);
        return corrispondingWf;
    }

    public static void readNAFFile(File naf,processNAFVariables vars) {

        try {
            JAXBContext jc = JAXBContext.newInstance("eu.fbk.knowledgestore.populator.naf.model");
            Unmarshaller unmarshaller = jc.createUnmarshaller();
			byte[] bytes = ByteStreams.toByteArray(IO.read(naf.getAbsolutePath()));
			vars.doc = (NAF) unmarshaller.unmarshal(new ByteArrayInputStream(bytes));
        } catch (UnsupportedEncodingException e) {
            logError(e.getMessage(),vars);
        } catch (FileNotFoundException e) {
            logError(e.getMessage(),vars);
		} catch (IOException e) {
			logError(e.getMessage(),vars);
        } catch (JAXBException e) {
            logError(e.getMessage(),vars);
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
