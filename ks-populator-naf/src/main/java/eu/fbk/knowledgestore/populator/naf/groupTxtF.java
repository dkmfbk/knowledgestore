package eu.fbk.knowledgestore.populator.naf;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import javax.xml.bind.JAXBException;

public class groupTxtF {
	static OutputStream out;
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
		
		analyzePathAndRunSystem(args[0]);
	}
	private static void analyzePathAndRunSystem(String path) throws JAXBException, IOException, InstantiationException, IllegalAccessException, NoSuchMethodException, SecurityException, ClassNotFoundException {
		File filePath = new File(path);
		if(filePath.exists()&&filePath.isDirectory()){
			//create report file in the same directory of running the system
			  out = new FileOutputStream(new File(filePath.getPath(), "groupedReport.txt"));

			 File[] listOfFiles = filePath.listFiles(); 
			  for (int i = 0; i < listOfFiles.length; i++) 
			  {
				 // System.out.println(listOfFiles[i].getName());
			   if (listOfFiles[i].isFile()&&listOfFiles[i].getName().endsWith(".txt")) 
			   {
				 System.err.println(i+"="+listOfFiles[i].getName());
			   //out.append("\n"+i+"="+listOfFiles[i].getName()+"\n");
				 copyFile(listOfFiles[i]);
			   }
			   out.flush();
			   System.gc();
			   Runtime.getRuntime().gc();
			  }
			  out.flush();
			  out.close();
		}else if(filePath.exists()&&filePath.isFile()) {
			System.err.println("It isn't efficient to group one file in one file");
		}
		out.flush();
		out.close();
	}
	 static void copyFile(File source) {
	        if (source == null || !source.exists())
	            return;
	        try{

	            InputStream inStream = new FileInputStream(source);

	            byte[] buffer = new byte[1024];

	            int length;
	            while ((length = inStream.read(buffer)) > 0) {
	                out.write(buffer, 0, length);
	            }

	            inStream.close();
	            out.flush();
	            //System.out.println("File copied into " + target);
	        }catch(IOException e) {
	            e.printStackTrace();
	        }
	    }

}
