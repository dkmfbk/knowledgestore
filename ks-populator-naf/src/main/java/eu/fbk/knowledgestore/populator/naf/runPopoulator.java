package eu.fbk.knowledgestore.populator.naf;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

import javax.xml.bind.JAXBException;

public class runPopoulator {

    /**
     * @param args
     * @throws IOException
     * @throws JAXBException
     * @throws ClassNotFoundException
     * @throws SecurityException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static void main(String[] args) throws InstantiationException, IllegalAccessException,
            NoSuchMethodException, SecurityException, ClassNotFoundException, JAXBException,
            IOException {
        //PrintStream PrintStream = new PrintStream(new File("log.txt"));
       // System.setOut(PrintStream) ;
       // System.setErr(PrintStream);
        // String path="/Users/qwaider/Desktop/NewsReader/download/coreset_13-19/";
        // String path="/Users/qwaider/Desktop/NewsReader/download/coreset_8_9_10_11_12/";
        // String path="/Users/qwaider/Desktop/NewsReader/time.xml";
        // path="/Users/qwaider/Desktop/NewsReader/download(1)/coreset_13-19/"+"5283-38T1-F0JC-M08P.xml.naf";
        String[] argt = {
               // "-d","/mnt/wind/work/test-20/2011"
               // , "-x", "Entity"
               // , "-u", "http://localhost:8080/"
              //  "-u","http://maya:8080",
              //  "-n","/Users/qwaider/Desktop/NewsReader/testSet/58B2-1JT1-DYTM-916F.xml_09f6656a977d7216bde58d33a5c51921.naf"
                 "-d","/Users/qwaider/Documents/Projects/NWR-August-v3/v3_test_dataset/test/"
             //   "-d","/Users/qwaider/Desktop/NewsReader/testMTH/DATA/",
             //   "-d","/Users/qwaider/Desktop/NewsReader/test3/"
               // "-n","/Users/qwaider/Desktop/NewsReader/test3/test.naf"
               // "-d","/Users/qwaider/Desktop/NewsReader/populator.test/test-multifiles/input/"
             //   "-x", "Entity" 
                ,"-p"
               // ,"-f","NAF_TESTWC.20.list"
                ,"-o", "rr-last2.report"
                ,"-or", "rr2.records" 
               // "-h"
                ,"-b","5"
                ,"-qs","5"
                //,"-ct","5"
                };
        nafPopulator tt = new nafPopulator();
        tt.main(argt);
    }

}
