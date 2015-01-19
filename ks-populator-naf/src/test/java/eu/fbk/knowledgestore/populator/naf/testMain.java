package eu.fbk.knowledgestore.populator.naf;

import java.io.IOException;

import javax.xml.bind.JAXBException;

import eu.fbk.knowledgestore.populator.naf.nafPopulator;

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
	public static void main(String[] args) throws InstantiationException, IllegalAccessException, NoSuchMethodException, SecurityException, ClassNotFoundException, JAXBException, IOException {
		String[] argt= 
		{
		"-n","/Users/qwaider/Desktop/NewsReader/testSet/58B2-1JT1-DYTM-916F.xml_09f6656a977d7216bde58d33a5c51921.naf"
		,"-x","Entity"
		};
		nafPopulator tt=new nafPopulator();
		tt.main(argt);

	}

}
