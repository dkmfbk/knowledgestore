package eu.fbk.knowledgestore.populator.naf;
import java.io.IOException;
import java.io.Writer;
import java.util.Hashtable;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;

public class Producer implements Runnable {
 
    static BlockingQueue<Hashtable<String, KSPresentation>> queue;
     
    public Producer(BlockingQueue<Hashtable<String, KSPresentation>> q){
        this.queue=q;
    }
    
    @Override
    public void run() {
        //produce messages
            NAFRunner msg = new NAFRunner();
                msg.generate();
               // Thread.sleep(i);
                //queue.put(msg);
                //System.out.println("Produced "+msg.getMsg());
           
        
    }
    
    
 
}