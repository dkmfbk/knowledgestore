package eu.fbk.knowledgestore.populator.naf;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Hashtable;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import eu.fbk.knowledgestore.Session;
import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Record;

public class Consumer implements Runnable {

    private BlockingQueue<Hashtable<String, KSPresentation>> queue;

    public Consumer(BlockingQueue<Hashtable<String, KSPresentation>> q) {
        this.queue = q;
    }
    static boolean called = false;
    static int cc=1;
    @Override
    public void run() {
        try {
            System.out.println("Start Consumer:"+cc);
            cc++;
           
            // consuming messages until exit message is received
                while (!nafPopulator.JobFinished||queue.size()>0) {
                    // Thread.sleep(10);
                    // System.out.println("Consumed "+msg.getMsg());
                   // System.out.println("submitting Consumer");
                    Hashtable<String, KSPresentation> obl = queue.poll();
                    if(obl!=null){
                    if (!nafPopulator.printToFile) {
                        submitCollectedData(obl);
                        
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
    }
    
  private boolean allThreadsDied() {
      boolean notFinished = true;
       for(Thread tmp: nafPopulator.threads){
           if(tmp.isAlive())
               notFinished=false;
           System.err.println("Threads("+tmp.getId()+") - alive:"+tmp.isAlive());
       }
       
       System.err.println("Threads alive check: "+notFinished);
        return notFinished;
    }

void  footer() throws IOException{
        if(!called&&(nafPopulator.JobFinished&&queue.isEmpty())){
            called=true;
        nafPopulator.out.append("Global stats:\n").append(nafPopulator.globalStats.getStats());
        nafPopulator.out.flush();
        
        if (!nafPopulator.printToFile&&(nafPopulator.JobFinished||queue.isEmpty())) {
            nafPopulator.closeConnection();
        }else{
            nafPopulator.mentionFile.flush();
            nafPopulator.mentionFile.close();
        }
       // out.append("Global stats:\n").append(globalStats.getStats());
        //out.flush();
        nafPopulator.out.close();
        nafPopulator.nullObjects();
        }
    }

    private static void submitCollectedData(Hashtable<String, KSPresentation> mentions) throws ClassNotFoundException,
            InstantiationException, IllegalAccessException, NoSuchMethodException,
            SecurityException, IOException {
        String className = "eu.fbk.knowledgestore.populator.naf.submitKS";
        Class clazz = Class.forName(className);
        Class[] parameters = new Class[] { Hashtable.class, boolean.class, Session.class };
        Method method = clazz.getMethod("init", parameters);
        Object obj = clazz.newInstance();
        try {
            nafPopulator.checkSession();
            int status = (Integer) method.invoke(obj, mentions, nafPopulator.store_partial_info, nafPopulator.session);
            if (status == 1) {
                for (KSPresentation ksF : mentions.values()) {
                    nafPopulator.out.append("NAF: " + ksF.getNaf_file_path()).append(ksF.getStats().getStats())
                            .append("\n");
                    nafPopulator.out.flush();
                    nafPopulator.updatestats(ksF.getStats());
                }
            }
            if (status == 0) {
                // error happens, rollback done. redo file by file,
                Hashtable<String, KSPresentation> mentmp = new Hashtable<String, KSPresentation>();
                for (Entry<String, KSPresentation> rc : mentions.entrySet()) {
                    mentmp.put(rc.getKey(), rc.getValue());

                    int status2 = (Integer) method
                            .invoke(obj, mentmp, nafPopulator.store_partial_info, nafPopulator.session);
                    if (status2 == 1) {
                        nafPopulator.out.append("NAF: " + rc.getValue().getNaf_file_path())
                                .append(rc.getValue().getStats().getStats()).append("\n");
                        nafPopulator.out.flush();
                        nafPopulator.updatestats(rc.getValue().getStats());
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
        if (nafPopulator.mentionFile == null) {
            nafPopulator.mentionFile = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(
                    nafPopulator.mentionsF)), "utf-8"));
        }
        for (Entry<String, KSPresentation> mn : mentions.entrySet()) {
            nafPopulator.out.append("NAF: " + mn.getValue().getNaf_file_path())
                    .append(mn.getValue().getStats().getStats()).append("\n");
            nafPopulator.out.flush();
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
        nafPopulator.out.flush();
        nafPopulator.mentionFile.flush();
        mentions.clear();

    }
}
