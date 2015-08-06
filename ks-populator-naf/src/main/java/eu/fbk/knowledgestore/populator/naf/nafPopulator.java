package eu.fbk.knowledgestore.populator.naf;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import eu.fbk.knowledgestore.KnowledgeStore;
import eu.fbk.knowledgestore.OperationException;
import eu.fbk.knowledgestore.Session;
import eu.fbk.knowledgestore.client.Client;
import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.populator.naf.connection.KnowledgestoreServer;
import eu.fbk.knowledgestore.vocabulary.KS;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class nafPopulator {

    public static int KSresourceReplacement = 1;     	//Default 1=discard the new, 2=ignore repopulate, 3=delete repopulate
	static statistics globalStats = new statistics();
    static Writer out, mentionFile;
    static int batchSize = 1, consumer_threads = 1;
    
    static String disabledItems = "", reportFileName = "report.txt", mentionsF = "records.txt";
    static boolean recursion = false, printToFile = false,JobFinished=false;
    static boolean store_partial_info = false;
    static boolean FInFile=false; //to keep track if the input is a file containing paths of NAFs
    static boolean ZInFile=false; //to keep track if the input is a zip archive containing NAF files
    static boolean TInFile=false; //to keep track if the input is a compressed tar archive containing NAF files
    static String INpath="";
    private static String SERVER_URL = "";
     static String USERNAME = "";
     static String PASSWORD = "";
    static Session session = null;
    static KnowledgeStore store = null;

   // static Hashtable<String, KSPresentation> mentions = new Hashtable<String, KSPresentation>();
    static String populatorVersion = "V0.1";
    static Logger logger = LoggerFactory.getLogger(nafPopulator.class);
    static LinkedList<Thread> threads = new LinkedList<Thread>();
    //Creating BlockingQueue of size 10
    static BlockingQueue<Hashtable<String, KSPresentation>> queue ;
    static Producer producer ;
    static Consumer consumer ;
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
    public static void main(String[] args) throws JAXBException, IOException,
            InstantiationException, IllegalAccessException, NoSuchMethodException,
            SecurityException, ClassNotFoundException {
         
       init();
        // Configure command line options
        final Options options = new Options();
        options.addOption("u", "ks_server_url", true, "the URL of the ks server");
        options.addOption("n", "NAF_file", true, "the path to a NAF file to be processed.");
        options.addOption("d", "NAF_directory", true,
                "the path of a directory whose files are NAF files to be processed.");
        options.addOption("f", "file", true,
                "the path of a file whose content is a list of NAF paths to be processed (one for line).");
        options.addOption("r", "recursive", false,
                "process recursively the given NAF directory (in conjunction with -d)");
        options.addOption("x", "exclude", true,
                "the given layer is excluded wen populating the K. Currently only the 'Entity' layer can be provided as argument.");
        options.addOption("b", "batchsize", true,
                "the number of NAF files to be processed and submitted to the KS in a single step; -1 means all (WARNING: very memory consuming!), defaults to 1.");
        options.addOption("qs", "queueSize", true,
                "the number of batch queue items to be hold in memory; defaults to 2.");
        options.addOption("ct", "consumerThreads", true,
                "the number of consumer threads to be thrown simultaneously, 1 is default.");
        options.addOption("ksm", "ksModality", true,
                "Submitting to KS modality:  (1=discard the new,Default) , (2=ignore previous content and populate), (3=delete previous content and repopulate)");

        options.addOption("v", "version", false,
                "display version and copyright information, then exit");
        options.addOption("h", "help", false, "display usage information, then exit");
        options.addOption("spi", "store_partial_info", false,
                "store in the KS even partial information in case of error (try to maximize data stored), defaults to false.");
        options.addOption("o", "outputreportfilepath", true,
                "the path of the 'report' file, where individual and overall statistics are saved");
        options.addOption("or", "outputrecordsfilepath", true,
                "the path of the 'record' file, where mentions and resources objects are saved");
        options.addOption("p", "parsingOnly", false,
                "perform NAF parsing only (do not store information in the KS)");
        options.addOption("z", "zip", true,
                "the path to a zip archive containing NAF files to be processed.");
        options.addOption("t", "tgz", true,
                "the path to a compressed tar archive (.tar.gz or .tgz) containing NAF files to be processed.");

        try {
            final CommandLine cmd = new GnuParser().parse(options, args);
	    {
		//check if we have many inputs in the same call, error and exit
		int nafFileModalitiesCount = 0;
		if (cmd.hasOption("ksm")) { 
			KSresourceReplacement=Integer.parseInt(cmd.getOptionValue("ksm")) ;
			}
		
		if (cmd.hasOption("n")) { nafFileModalitiesCount++;}
		if (cmd.hasOption("d")) { nafFileModalitiesCount++;}
		if (cmd.hasOption("f")) { nafFileModalitiesCount++;}
		if (cmd.hasOption("z")) { nafFileModalitiesCount++;}
		if (nafFileModalitiesCount > 1) {
		    System.err.println("Cannot manage multiple options(-n|-d|-f|-z): please choice one of them.");  
		    printUsage(options);
		    System.exit(0);
		}
            }

            if (cmd.hasOption("u")) {
                SERVER_URL = cmd.getOptionValue('u');
            }
            
            if (cmd.hasOption("ct")) {
                consumer_threads=Integer.parseInt(cmd.getOptionValue("ct"))  ;
            }
            //TODO important if any illegal input crash with error message
            
            if (cmd.hasOption("qs")) {
                queue = new ArrayBlockingQueue<>(Integer.parseInt(cmd.getOptionValue("qs")))  ;
            }else{
                queue = new ArrayBlockingQueue<>(2)  ;
            }
             producer = new Producer(queue);
             consumer = new Consumer(queue);
            
            if (cmd.hasOption("o")) {
                reportFileName = cmd.getOptionValue('o');
                File tst = new File(reportFileName);
                if(tst.exists()&&!tst.isFile()&&tst.isDirectory()){
                    reportFileName = reportFileName +"/report.txt";
                }
                nafPopulator.out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(
                        // filePath.getPath(),
                                    nafPopulator.reportFileName)), "utf-8"));
            }
            if (cmd.hasOption("or")) {
                mentionsF = cmd.getOptionValue("or");
                File tst = new File(mentionsF);
                if(tst.exists()&&!tst.isFile()&&tst.isDirectory()){
                    mentionsF = mentionsF +"/records.txt";
                }
                nafPopulator.mentionFile = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(
                        // filePath.getPath(),
                                    nafPopulator.mentionsF)), "utf-8"));
            }
            
            if (cmd.hasOption("p")) {
                printToFile = true;
            }
            
            if (cmd.hasOption("spi")) {
                store_partial_info = true;
            }
            if (cmd.hasOption("v")) {
                System.out.println("KnowledgeStore.populator.version: " + populatorVersion);
                System.exit(0);
            }
            if (cmd.hasOption("h")
		|| (!cmd.hasOption("n") && !cmd.hasOption("d") && !cmd.hasOption("f") && !cmd.hasOption("z") && !cmd.hasOption("t"))) {
                printUsage(options);
                System.exit(0);
            }

            if (cmd.hasOption("b")) {
                batchSize = Integer.parseInt(cmd.getOptionValue('b'));
            } else {
                batchSize = 1;
            }

            if (cmd.hasOption("x")) {
                disabledItems = cmd.getOptionValue('x');
            }
            if (cmd.hasOption("r")) {
                recursion = true;
            }
            if (!printToFile && 
		(cmd.hasOption("n") || cmd.hasOption("d") || cmd.hasOption("f") || cmd.hasOption("z") || cmd.hasOption("t"))) {
                readConnectionFile();
            }
            if (cmd.hasOption("n") || cmd.hasOption("d") || cmd.hasOption("f") || cmd.hasOption("z") || cmd.hasOption("t")) {
            
                if (cmd.hasOption("n")){
                    INpath = cmd.getOptionValue('n');
                    //analyzePathAndRunSystem(cmd.getOptionValue('n'), disabledItems, recursion);
                } else if (cmd.hasOption("d")){
                    INpath = cmd.getOptionValue('d');
                   // analyzePathAndRunSystem(cmd.getOptionValue('d'), disabledItems, recursion);
                } else if (cmd.hasOption("f")) {
                    FInFile=true;
                    INpath = cmd.getOptionValue('f');
                } else if (cmd.hasOption("z")) {
                    ZInFile=true;
                    INpath = cmd.getOptionValue('z');
                }  else if (cmd.hasOption("t")) {
                    TInFile=true;
                    INpath = cmd.getOptionValue('t');
                }
                //starting producer to produce messages in queue
                new Thread(producer).start();
                //starting consumer to consume messages from queue
              /*  ExecutorService threadPool = Executors.newFixedThreadPool(consumer_threads);
                threadPool.submit(consumer);
                threadPool.shutdown();*/
               for(int i=0;i<consumer_threads;i++){
                Thread a = new Thread(consumer);
                a.start();
                threads.addLast(a);
                }
              
                // new Thread(consumer).start();
               finalizeThread finalizeThreadObj = new finalizeThread();
                new Thread(finalizeThreadObj).start();
               
            
            }

        } catch (final ParseException ex) {
            // Display error message and then usage on syntax error
            System.err.println("SYNTAX ERROR: " + ex.getMessage());
            printUsage(options);
        } catch (final Throwable ex) {
            // Display error message and stack trace on generic error
            System.err.print("EXECUTION FAILED: ");
            ex.printStackTrace();
            printUsage(options);
        } 
    
    
    }

    private static void init() {
        globalStats = new statistics();
        out=null;
        mentionFile = null;
        disabledItems = ""; reportFileName = "report.txt"; mentionsF = "records.txt";
        recursion = false; printToFile = false;
        store_partial_info = false;
        SERVER_URL = "";
        USERNAME = "";
        PASSWORD = "";
        session = null;
        store = null;
        //mentions = new Hashtable<String, KSPresentation>();
        logger = LoggerFactory.getLogger(nafPopulator.class);

    }

    static void nullObjects() throws IOException {
        nafPopulator.closeConnection();
             nafPopulator.mentionFile.flush();
             nafPopulator.mentionFile.close();
             nafPopulator.out.flush();
         nafPopulator.out.close();
        globalStats = null;
        out=null;
        mentionFile = null;
        batchSize = 1;
        disabledItems = null;
        reportFileName = null;
        mentionsF = null;
        recursion = false;
        printToFile = false;
        store_partial_info = false;
        session = null;
        store = null;
       // mentions = null;
        
    }

    private static void printUsage(Options options) {
        int WIDTH = 80;
        final PrintWriter out = new PrintWriter(System.out);
        final HelpFormatter formatter = new HelpFormatter();
        formatter.printUsage(out, WIDTH, "eu.fbk.knowledgestore.populator.naf.nafPopulator",
                options);
        out.println("\nOptions");
        formatter.printOptions(out, WIDTH, options, 2, 2);
        out.flush();
    }




    static void updatestats(statistics st) {
        globalStats.setObjectMention(globalStats.getObjectMention() + st.getObjectMention());
        globalStats.setPER(globalStats.getPER() + st.getPER());
        globalStats.setORG(globalStats.getORG() + st.getORG());
        globalStats.setLOC(globalStats.getLOC() + st.getLOC());
        globalStats.setFin(globalStats.getFin() + st.getFin());
        globalStats.setMix(globalStats.getMix() + st.getMix());
        globalStats.setPRO(globalStats.getPRO() + st.getPRO());
        globalStats.setNo_mapping(globalStats.getNo_mapping() + st.getNo_mapping());
        globalStats.setTimeMention(globalStats.getTimeMention() + st.getTimeMention());
        globalStats.setEventMention(globalStats.getEventMention() + st.getEventMention());
        globalStats.setParticipationMention(globalStats.getParticipationMention()
                + st.getParticipationMention());
        globalStats.setEntity(globalStats.getEntity() + st.getEntity());
        globalStats.setCoref(globalStats.getCoref() + st.getCoref());
        globalStats.setFactuality(globalStats.getFactuality() + st.getFactuality());
        globalStats.setRole(globalStats.getRole() + st.getRole());
        globalStats.setRolewithEntity(globalStats.getRolewithEntity() + st.getRolewithEntity());
        globalStats.setRolewithoutEntity(globalStats.getRolewithoutEntity()
                + st.getRolewithoutEntity());
        globalStats.setSrl(globalStats.getSrl() + st.getSrl());
        globalStats.setTimex(globalStats.getTimex() + st.getTimex());
        globalStats.setTlinkMention(globalStats.getTlinkMention() + st.getTlinkMention());
        globalStats.setTlinkMentionDiscarded(globalStats.getTlinkMentionDiscarded() + st.getTlinkMentionDiscarded());
        globalStats.setClinkMention(globalStats.getClinkMention() + st.getClinkMention());
        globalStats.setClinkMentionDiscarded(globalStats.getClinkMentionDiscarded() + st.getClinkMentionDiscarded());
        globalStats.setTlinkMentionsEnriched(globalStats.getTlinkMentionsEnriched() + st.getTlinkMentionsEnriched());

        globalStats.setCorefMentionEvent(globalStats.getCorefMentionEvent() + st.getCorefMentionEvent());
        globalStats.setCorefMentionNotEvent(globalStats.getCorefMentionNotEvent() + st.getCorefMentionNotEvent());
    }

    public static void readConnectionFile() throws UnsupportedEncodingException, JAXBException,
            IOException {
        String resourceName = "populator-ks-connection.xml";
        URL url = nafPopulator.class.getResource(resourceName);

        JAXBContext jc = JAXBContext.newInstance("eu.fbk.knowledgestore.populator.naf.connection");
        Unmarshaller unmarshaller = jc.createUnmarshaller();
        if (url != null) {
            KnowledgestoreServer myFile = (KnowledgestoreServer) unmarshaller
                    .unmarshal(new InputStreamReader(url.openStream(), "UTF-8"));
	    // read SERVER_URL from XML file unless previously defined with command line parameters
	    if (SERVER_URL.equals("")) {
		SERVER_URL = myFile.getUrl();
	    }
            USERNAME = myFile.getUsername();
            PASSWORD = myFile.getPassword();
            checkSession();
        } else {
            System.err
                    .println("Error: populator-ks-connection.xml.xml file not found!\nYou should first create the connection file to the KS.");
        }

    }

    static void checkSession() {
	logger.info("checkSession SERVER_URL |" + SERVER_URL + "|");
        if (store == null) {
            // Initialize a KnowledgeStore client
            store = Client.builder(SERVER_URL).maxConnections(16).validateServer(false).build();
        }
        if (store != null && (session == null || session.isClosed())) {
            // Acquire a session for a given username/password pair
            session = store.newSession(USERNAME, PASSWORD);
        }
        if (store == null || session == null || session.isClosed()) {
	    String errMsg = "";
	    if (store == null) { errMsg = "null store"; }
	    else if (session == null) { errMsg = "null session"; }
	    else { errMsg = "closed session"; }
            logger.error("checkSession with SERVER_URL " + SERVER_URL + " : " + errMsg);
            System.exit(0);
        }
        try {
            session.download(new URIImpl("http://localhost/test")).exec();
        } catch (IllegalStateException e) {
            e.printStackTrace();
            logger.error("checkSession with SERVER_URL " + SERVER_URL + " : IllegalStateException");
            System.exit(0);
        } catch (OperationException e) {
            e.printStackTrace();
            logger.error("checkSession with SERVER_URL " + SERVER_URL + " : OperationException");
            System.exit(0);
        }

    }

    static void closeConnection() {
    	if(session!=null && !session.isClosed()){
        // Close the session
        session.close();
    	}
    	if(store!=null&&!store.isClosed()){
        // Ensure to close the KS (will also close pending sessions)
        store.close();
    	}
    }
}
