package eu.fbk.knowledgestore.populator.naf;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Hashtable;
import java.util.LinkedList;

public class sumStatistics {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		//String inputFile="/Users/qwaider/Desktop/NewsReader/download/coreset_13-19/report.txt";
		String inputFile="/Users/qwaider/Desktop/NewsReader/download/coreset_8_9_10_11_12/report.txt";

		
		FileInputStream in = new FileInputStream(inputFile);
        Reader reader = new InputStreamReader(in, "utf-8");
        BufferedReader br = new BufferedReader(reader);
        String line;
       int NumberOfFiles= 0;
        int Entity=0;
        int Coreference=0;
        int Srl=0;
        int Participation=0;
        int RoleWithEntity=0;
        int RoleWithoutEntity=0;
        int Timex=0;
        int Factuality=0;
        int discarded=0;
        int i=0;
        while((line = br.readLine()) != null){
        	if(line.length()>0){
        	if(!line.contains("null")){
        	switch(i){
        	case 0: NumberOfFiles++; break;
        	case 1:break;
        	case 2: Entity+=getNumber("Entity:",line); break;
        	case 3: Coreference+=getNumber("Coreference:",line); break;
        	case 4: Srl+=getNumber("Srl:",line); break;
        	case 5: Participation+=getNumber("Participation:",line); break;
        	case 6: RoleWithEntity+=getNumber("Role with entity=",line); break;
        	case 7: RoleWithoutEntity+=getNumber("Role without entity=",line); break;
        	case 8: Factuality+=getNumber("Timex:",line); break;
        	case 9: Factuality+=getNumber("Factuality:",line); break;
        	}
        	if(i<9)
        		i++;
        	else
        		i=0;
        	}else{
        		discarded++;
        		i=0;
        		br.readLine();//an empty line
        	}
        	}
        }
        
        System.out.println("Number of involved Files: "+NumberOfFiles+"\nNumber of discarded files: "+discarded+"\nExtracted mentions:\nEntity:"+Entity+"\nCoreference:"+Coreference+"\nSrl:"+Srl);
		System.out.println("\nParticipation:"+Participation);
		System.out.println("\nRole with entity="+RoleWithEntity);
		System.out.println("\nRole without entity="+RoleWithoutEntity+"\nTimex: "+Timex+"\nFactuality:"+Factuality);
       

	}

	private static int getNumber(String head, String line) {
		if(line.startsWith(head)){
			line=line.replace(head, "");
			return Integer.parseInt(line);
		}else{
			System.err.println("Head: "+head+"=line="+line);
		}
		return 0;
	}

}
