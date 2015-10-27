package eu.fbk.knowledgestore.populator.naf;

import eu.fbk.knowledgestore.Session;
import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Record;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Hashtable;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;

public class Consumer implements Runnable {

    private BlockingQueue<Hashtable<String, KSPresentation>> queue;

    public Consumer(BlockingQueue<Hashtable<String, KSPresentation>> q) {
        this.queue = q;
    }
    static boolean called = false;
    static int cc=1;
    @Override
    public void run() {
        Session session = null;
        try {
            System.out.println("Start Consumer:"+cc);
            int cN= cc;
            cc++;
            if(nafPopulator.store!=null&&!nafPopulator.store.isClosed()){
            	session = nafPopulator.store.newSession(nafPopulator.USERNAME, nafPopulator.PASSWORD);
            }
            // consuming messages until exit message is received
                while (!nafPopulator.JobFinished||queue.size()>0) {
                    // Thread.sleep(10);
                    // System.out.println("Consumed "+msg.getMsg());
                   // System.out.println("submitting Consumer");
                    Hashtable<String, KSPresentation> obl = queue.poll();
                    if(obl!=null){
                       System.out.println("Consumer:"+cc+" is serving{"+obl.keySet()+"}");
                    if (!nafPopulator.printToFile) {
                        submitCollectedData(obl,session);
                        
                    } else {
                        appendCollectedDataToFile(obl);
                        
                    }
                    }
                   /* try 
                    {
                     // 
                        Hashtable<String, KSPresentation> obl = queue.poll(100, TimeUnit.MILLISECONDS);
                        if(nafPopulator.JobFinished && queue.isEmpty()&&obl==null)
                            return;
                        if(obl!= null)
                        {
                           
                        }
                    } 
                    catch (InterruptedException e) 
                    {                   
                        return;
                    }*/

                }
                
                /*Thread.currentThread().interrupt();
                
                if(allThreadsDied())
                    footer();*/
            
                
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException | SecurityException | IOException  e) {
            e.printStackTrace();
        }
        finally {
            if (session != null) {
                session.close();
            }
        }
    }
    



    private static void submitCollectedData(Hashtable<String, KSPresentation> mentions, Session session) throws ClassNotFoundException,
            InstantiationException, IllegalAccessException, NoSuchMethodException,
            SecurityException, IOException {
        String className = "eu.fbk.knowledgestore.populator.naf.submitKS";
        Class clazz = Class.forName(className);
        Class[] parameters = new Class[] { Hashtable.class, boolean.class, Session.class };
        Method method = clazz.getMethod("init", parameters);
        Object obj = clazz.newInstance();
        try {

            nafPopulator.checkSession();
            int status = (Integer) method.invoke(obj, mentions, nafPopulator.store_partial_info, session);
            if (status == 1) {// resourse submitted and states updated in submitKS.java
            }
            if (status == 0) {
                // error happens, rollback done. redo file by file,
                Hashtable<String, KSPresentation> mentmp = new Hashtable<String, KSPresentation>();
                for (Entry<String, KSPresentation> rc : mentions.entrySet()) {
                    mentmp.put(rc.getKey(), rc.getValue());

                    int status2 = (Integer) method
                            .invoke(obj, mentmp, nafPopulator.store_partial_info, nafPopulator.session);
                    if (status2 == 1) {
                    	// resourse submitted and states updated in submitKS.java
                        mentmp.clear();
                    }
                    if (status2 == 0) {
                        nafPopulator.logger.error("Error storing this file to KS: "
                                + rc.getValue().getNaf_file_path());
                    }

                }
            }
            mentions.clear();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            String error = " Involved file(s):";
            for (KSPresentation vl : mentions.values()) {
                error += vl.getNaf_file_path() + ",";
            }
            error += ((e.getMessage() != null) ? e.getMessage() : "")
                    + "\nStoring to KS phase: Populating mentions interrupted!";
            nafPopulator.logger.error(error);
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            String error = " Involved file(s):";
            for (KSPresentation vl : mentions.values()) {
                error += vl.getNaf_file_path() + ",";
            }
            error += ((e.getMessage() != null) ? e.getMessage() : "")
                    + "\nStoring to KS phase: Populating mentions interrupted!";
            nafPopulator.logger.error(error);
        } catch (InvocationTargetException e) {
            e.printStackTrace();
            String error = " Involved file(s):";
            for (KSPresentation vl : mentions.values()) {
                error += vl.getNaf_file_path() + ",";
            }
            error += ((e.getMessage() != null) ? e.getMessage() : "")
                    + "\nStoring to KS phase: Populating mentions interrupted!";
            nafPopulator.logger.error(error);
        } finally {
            mentions.clear();
        }
    }

    private static void appendCollectedDataToFile(Hashtable<String, KSPresentation> mentions) throws IOException {

        for (Entry<String, KSPresentation> mn : mentions.entrySet()) {
            String naf_file_path = mn.getValue().getNaf_file_path();
            String stats = mn.getValue().getStats().getStats();
            if (nafPopulator.out != null) {
                nafPopulator.out.append("NAF: " + naf_file_path);
                nafPopulator.out.append(stats);
                nafPopulator.out.append("\n");
                nafPopulator.out.flush();
            }
            nafPopulator.updatestats(mn.getValue().getStats());
            nafPopulator.mentionFile.append(mn.getValue().getNewsResource()
                    .toString(Data.getNamespaceMap(), true)
                    + "\n");
            nafPopulator.mentionFile.append(mn.getValue().getNaf().toString(Data.getNamespaceMap(), true)
                    + "\n");
            for (Record mnMen : mn.getValue().getMentions().values()) {
                nafPopulator.mentionFile.append(mnMen.toString(Data.getNamespaceMap(), true) + "\n");
            }

        }
        if (nafPopulator.out != null) {
            nafPopulator.out.flush();
        }
        nafPopulator.mentionFile.flush();
        mentions.clear();

    }
}
