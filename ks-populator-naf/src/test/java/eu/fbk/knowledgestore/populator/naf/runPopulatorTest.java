package eu.fbk.knowledgestore.populator.naf;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;

import javax.xml.bind.JAXBException;

public class runPopulatorTest {

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

        // String path="/Users/qwaider/Desktop/NewsReader/download/coreset_13-19/";
        // String path="/Users/qwaider/Desktop/NewsReader/download/coreset_8_9_10_11_12/";
        // String path="/Users/qwaider/Desktop/NewsReader/time.xml";
        // path="/Users/qwaider/Desktop/NewsReader/download(1)/coreset_13-19/"+"5283-38T1-F0JC-M08P.xml.naf";
        System.out.println("Run single file test:");
        runTest("single/input/test.naf", "single/output/", "single/gold/");
        System.out.println("Run multi-files test:");
        runTest("multifiles/input/", "multifiles/output/", "multifiles/gold/");
    }

    private static void runTest(String input, String output, String gold)
            throws InstantiationException, IllegalAccessException, NoSuchMethodException,
            SecurityException, ClassNotFoundException, JAXBException, IOException {
        URL url = runPopulatorTest.class.getResource(input);
        URL urlOut = runPopulatorTest.class.getResource(output);
        URL urlGold = runPopulatorTest.class.getResource(gold);
        PrintStream original = System.out;
        PrintStream PrintStream = new PrintStream(new File(urlOut.getPath() + "/log.txt"));
        System.setOut(PrintStream);
        System.setErr(PrintStream);

        String[] argt = { "-d", url.getPath(), "-x", "Entity", "-o", urlOut.getPath(), "-or",
                urlOut.getPath(), "-p" };
        nafPopulator tt = new nafPopulator();
        tt.main(argt);
        //runClass(argt);
        System.setOut(original);
        System.setErr(original);
        if (!compare(urlGold.getPath() + "/report.txt", urlOut.getPath() + "/report.txt"))
            System.out.println("report.txt: test passed!");
        else
            System.err.println("report.txt: test failed!");
        if (!compare(urlGold.getPath() + "/records.txt", urlOut.getPath() + "/records.txt"))
            System.out.println("records.txt: test passed!");
        else
            System.err.println("records.txt: test failed!");

        tt = null;
        System.gc();
        PrintStream.flush();
        PrintStream.close();

    }

    private static void runClass(String[] argt) throws InstantiationException,
            IllegalAccessException, NoSuchMethodException, SecurityException,
            ClassNotFoundException, IOException {
        String className = "eu.fbk.knowledgestore.populator.naf.nafPopulator";
        Class clazz = Class.forName(className);
        Class[] parameters = new Class[] { String[].class};
        Method method = clazz.getMethod("main", parameters);
        Object obj = clazz.newInstance();
        try {
           method.invoke(obj, new Object[]{argt});
            
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }

    }

    static boolean compare(String fileName1, String fileName2) throws IOException {
        FileInputStream fstream1 = new FileInputStream(fileName1);
        FileInputStream fstream2 = new FileInputStream(fileName2);

        DataInputStream in1 = new DataInputStream(fstream1);
        DataInputStream in2 = new DataInputStream(fstream2);

        BufferedReader br1 = new BufferedReader(new InputStreamReader(in1));
        BufferedReader br2 = new BufferedReader(new InputStreamReader(in2));

        String strLine1, strLine2;
        boolean problem = false;

        while ((strLine1 = br1.readLine()) != null && (strLine2 = br2.readLine()) != null) {
            if (!strLine1.equals(strLine2)) {
                problem = true;
            }
        }
        fstream1.close();
        fstream2.close();
        return problem;
    }
}
