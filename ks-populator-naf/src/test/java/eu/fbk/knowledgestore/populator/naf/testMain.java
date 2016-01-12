package eu.fbk.knowledgestore.populator.naf;

import javax.xml.bind.JAXBException;
import java.io.IOException;

public class testMain {

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
    public static void main(String[] args) throws Exception {
        nafPopulator tt = new nafPopulator();
        tt.main(args);

    }

}
