package eu.fbk.knowledgestore.populator.naf;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Set;

public class getToBeProcessed {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		Hashtable<String,String> all = new Hashtable<String, String>();
		Hashtable<String,String> sub = new Hashtable<String, String>();
		Hashtable<String,String> intersect = new Hashtable<String, String>();		
		String ap="/Users/qwaider/Documents/Projects/NWR-June2015/knowledgestore/ks-distribution/target/" +
				//"nafs.txt";
				"xmls.txt";
		String sp="/Users/qwaider/Documents/Projects/NWR-June2015/knowledgestore/ks-distribution/target/" +
				//"processedNAFS.txt";
				"processedxmls.txt";
		String intersectp="/Users/qwaider/Documents/Projects/NWR-June2015/knowledgestore/ks-distribution/target/" +
			//	"toBeProcessedNAFS.txt";
				"toBeProcessedXMLS.txt";
		readFile(ap,all);
		readFile(sp,sub);
		intersect(intersectp,all,sub,intersect);
		System.out.println("all: "+all.size()+"-sub:"+sub.size()+"-intersect:"+intersect.size());
	}
	
	private static void intersect(String intersectp, Hashtable<String, String> all,
			Hashtable<String, String> sub,Hashtable<String, String> intersect) throws IOException {
		 
		File file = new File(intersectp);

		// if file doesnt exists, then create it
		if (!file.exists()) {
			file.createNewFile();
		}

		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		for(String link : all.keySet()){
			if(!sub.containsKey(link)){
			bw.write(link+"\n");
			bw.flush();
			if(!intersect.containsKey(link))
			intersect.put(link, "");
			//System.out.println(link);
			}
			
		}
		bw.close();		
	}

	static void readFile(String filepath,Hashtable<String,String> fls){
		BufferedReader br = null;
		 
		try {
 
			String sCurrentLine;
 
			br = new BufferedReader(new FileReader(filepath));
 
			while ((sCurrentLine = br.readLine()) != null) {
				fls.put(sCurrentLine, "");
				//System.out.println(sCurrentLine);
			}
 
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}

}
