package eu.fbk.knowledgestore.populator.naf;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.knowledgestore.OperationException;
import eu.fbk.knowledgestore.Session;
import eu.fbk.knowledgestore.data.ParseException;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Representation;
import eu.fbk.knowledgestore.vocabulary.KS;

public class submitKS {
    private static Logger logger = LoggerFactory.getLogger(submitKS.class);

    public static Integer init(Hashtable<String, KSPresentation> batchList, boolean store_partical_info, Session session) throws IOException{
	    
	List<Record> resources=null;
	List<Record> mentions=null;
	try {
		checkResourceFoundInKS(session,batchList);
	    // Clear all resources stored in the KS
	    // session.delete(KS.RESOURCE).exec();

	    // Store resource records, in a single operation
	    resources = submitResources(batchList, session);
	    //upload news file
	    uploadNews(batchList, session);
	    // upload naf file
	    uploadNaf(batchList, session);
	    //store mentions records, in a single operation
	    mentions = submitMentions(batchList, session);
	    return 1;
	} catch (ParseException e) {
	    //e.printStackTrace();
	    //rollback
	    rollback(session,resources,mentions);
	    logger.error(e.getMessage(), e);
	    //inout.append(e.getMessage());
	    return 0;
	} catch (IllegalStateException e) {
	    //rollback
	    rollback(session,resources,mentions);
	    logger.error(e.getMessage(), e);
	    return 0;
	} catch (OperationException e) {
	    //rollback
	    logger.error(e.getMessage(), e);
	    rollback(session,resources,mentions);
            
	    return 0;
	} catch (IOException e) {
	    //rollback
	    rollback(session,resources,mentions);
	    logger.error(e.getMessage(), e);
	    return 0;
	}
		
    }
    
    private static boolean doSubmitToKS(KSPresentation tmp){
        //Default 1=discard the new, 2=ignore repopulate, 3=delete repopulate
    	if(nafPopulator.KSresourceReplacement==1&&!tmp.isFoundInKS())
    		return true;
    	if(nafPopulator.KSresourceReplacement==2)
    		return true;
    	if(nafPopulator.KSresourceReplacement==3){
    		if(tmp.isFoundInKS()){
    			//TODO do the deletion of the previous ks resources then return true;
    		}
    		return true;
    	}
    		
    	return false;
    }
	
    private static void checkResourceFoundInKS(
			Session session, Hashtable<String, KSPresentation> batchList) {
    	if(nafPopulator.KSresourceReplacement!=2){
    	for (KSPresentation tmp : batchList.values()) {
    	    try {
    			long cc = session.count(tmp.getNewsResource().getID()).exec();
    			if(cc>0)
    				tmp.setFoundInKS(true);
    			else
    				tmp.setFoundInKS(false);
    			
    		} catch (IllegalStateException e) {
    			tmp.setFoundInKS(false);
    		    logger.error(e.getMessage(), e);
    		} catch (OperationException e) {
    			tmp.setFoundInKS(false);
    		    logger.error(e.getMessage(), e);
    		}
    	  }
    	}
	}

	private static void rollback(Session session, List<Record> resources, List<Record> mentions){
	List<URI> idsIT;
	
	try {
	    // delete the resources by id
	    idsIT = new LinkedList<URI>();
	    for(Record rID : resources){
		idsIT.add(rID.getID());
	    }
	    session.delete(KS.RESOURCE).ids(idsIT).exec();

	    // delete the mentions by id
	    idsIT.clear();
	    for(Record rID : mentions){
		idsIT.add(rID.getID());
	    }
	    session.delete(KS.MENTION).ids(idsIT).exec();

        } catch (ParseException e) {
            logger.error("Error rollback: "+e.getMessage());
        } catch (IllegalStateException e) {
            logger.error("Error rollback: "+e.getMessage());
        } catch (OperationException e) {
            logger.error("Error rollback: "+e.getMessage());
        }
    }

    private static List<Record> submitMentions(Hashtable<String, KSPresentation> batchList, Session session) throws ParseException, IllegalStateException, OperationException {
	final List<Record> records = getAllMentionType(batchList);
	session.merge(KS.MENTION).criteria("overwrite *").records(records)
	    .exec();
	// Count and print the number of resources in the KS
	// final long numResources = session.count(KS.MENTION).exec();
	// System.out.println(numResources + " mentions in the KS");
	
	// return the list of submitted mentions
	return records;
    }

    private static List<Record> submitResources(Hashtable<String, KSPresentation> batchList, Session session) throws IllegalStateException, OperationException, IOException {
	final List<Record> records = getAllResourceType(batchList);
	session.merge(KS.RESOURCE).criteria("overwrite *").records(records)
	    .exec();
	// Count and print the number of resources in the KS
	// final long numResources = session.count(KS.RESOURCE).exec();
	// System.out.println(numResources + " resources in the KS");
	return records;
    }

    private static Hashtable<URI, String> uploadNaf(Hashtable<String, KSPresentation> batchList, Session session) throws IllegalStateException, OperationException {
	Hashtable<URI, String> FILE_RESOURCES = getAllNafResources(batchList);
	// Store resource files, one at a time
	for (final Map.Entry<URI, String> entry : FILE_RESOURCES.entrySet()) {
	    final URI resourceID = entry.getKey();
	    final File nafFile = new File(entry.getValue());
	    final Representation representation = Representation.create(nafFile);
	    try {
		session.upload(resourceID).representation(representation).exec();
	    } finally {
		representation.close();

		// delete the nafFile if it is a temporary file extracted from a zip or tgz archive
		if (nafPopulator.ZInFile || nafPopulator.TInFile) {
		    nafFile.delete();
		}
	    }
	}
	return FILE_RESOURCES;
    }

    private static Hashtable<URI, String> uploadNews(Hashtable<String, KSPresentation> batchList, Session session) throws IllegalStateException, OperationException {
	Hashtable<URI, String> FILE_RESOURCES = getAllResources(batchList);
	// Store resource files, one at a time
	for (final Map.Entry<URI, String> entry : FILE_RESOURCES.entrySet()) {
	    final URI resourceID = entry.getKey();
	    final Representation representation = Representation.create(entry
									.getValue());
	    try {
		session.upload(resourceID).representation(representation)
		    .exec();
	    } finally {
		representation.close();
	    }
	}
	return FILE_RESOURCES;
    }

    private static List<Record> getAllResourceType(Hashtable<String, KSPresentation> batchList) throws IOException {
	List<Record> temp = new LinkedList<Record>();
	for (KSPresentation tmp : batchList.values()) {
		if(doSubmitToKS(tmp)){
	    temp.add(tmp.getNewsResource());
	    temp.add(tmp.getNaf());
	    /*inout.append(tmp.getNewsResource().toString(Data.getNamespaceMap(),
	      true)
	      + "\n");
	      inout.append(tmp.getNaf().toString(Data.getNamespaceMap(), true)
	      + "\n");*/
		}
	}
	return temp;
    }

    private static Hashtable<URI, String> getAllNafResources(Hashtable<String, KSPresentation> batchList) {
	Hashtable<URI, String> temp = new Hashtable<URI, String>();
	for (KSPresentation tmp : batchList.values()) {
		if(doSubmitToKS(tmp)){
	    temp.put(tmp.getNaf().getID(), tmp.getNaf_file_path());
		}
	}
	return temp;
    }

    private static Hashtable<URI, String> getAllResources(Hashtable<String, KSPresentation> batchList) {
	Hashtable<URI, String> temp = new Hashtable<URI, String>();
	for (KSPresentation tmp : batchList.values()) {
		if(doSubmitToKS(tmp)){
	    temp.put(tmp.getNewsResource().getID(), tmp.getNews());
		}
	}
	return temp;
    }

    private static List<Record> getAllMentionType(Hashtable<String, KSPresentation> batchList) {
	List<Record> temp = new LinkedList<Record>();
	for (KSPresentation tmp : batchList.values()) {
		if(doSubmitToKS(tmp)){
	    temp.addAll(tmp.getMentions().values());
		}
	}
	return temp;
    }

}
