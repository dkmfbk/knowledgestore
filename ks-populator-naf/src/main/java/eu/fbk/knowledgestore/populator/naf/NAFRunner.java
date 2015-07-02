package eu.fbk.knowledgestore.populator.naf;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.xml.bind.JAXBException;

import org.apache.commons.compress.archivers.tar.*;
import org.apache.commons.compress.compressors.gzip.*;
import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;

public class NAFRunner {
    
    void generate(){
        try {
	    if (nafPopulator.FInFile) {
		/* 
		   input is a file whose content is a list of NAF paths to be processed (one for line)
		*/
		FileInputStream in = new FileInputStream(nafPopulator.INpath);
		Reader reader = new InputStreamReader(in, "utf8");
		BufferedReader br = new BufferedReader(reader);
		String line = "";
		LinkedList<File> fileslist = new LinkedList<File>();
		while ((line = br.readLine()) != null) {
            
		    if(fileslist.size() >= nafPopulator.batchSize){
			RunSystemOnList(fileslist, nafPopulator.disabledItems, nafPopulator.recursion);
			fileslist.clear();
		    }
           
		    File e=new File(line);
		    if(e.exists())
			fileslist.addLast(e);
		    else {
			System.err.println("Path not exist!" + e.getPath());

		    }
           
		}
		if(fileslist.size()>0){
		    RunSystemOnList(fileslist, nafPopulator.disabledItems, nafPopulator.recursion);
		    fileslist.clear();
		}
		in.close();
	    } else if (nafPopulator.ZInFile) {
		/* 
		   input is a zip archive containing NAF files to be processed
		*/
		String ZIP_OUTPUT_DIR = "/tmp/nafPopulatorZipOutDir";
		byte[] buffer = new byte[1024];
		LinkedList<File> fileslist = new LinkedList<File>();
		boolean multipleFileFlag = (nafPopulator.batchSize > 1);

		// create output directory is not exists
		File zipDir = new File(ZIP_OUTPUT_DIR);
		if (!zipDir.exists()) {
		    zipDir.mkdir();
		}

		// get the zip file content
		ZipInputStream zis = new ZipInputStream(new FileInputStream(nafPopulator.INpath));

		// iterate over zipped file list entry
		ZipEntry ze = zis.getNextEntry();
		while (ze != null) {

		    // if it is a directory, then skip it, else copy the file contents
		    //
		    if (ze.isDirectory()) {
			/*
			  String zeName = ze.getName();
			  File extractedDir = new File(ZIP_OUTPUT_DIR + File.separator + zeName);
			  extractedDir.mkdirs();
			  System.out.println("ROL2: created new dir " + extractedDir.getAbsoluteFile());
			*/
		    } else {

			String zeName = ze.getName();

			// just use the basename of the file
			File tmpFile = new File(ZIP_OUTPUT_DIR + File.separator + zeName);
			String basename = tmpFile.getName();

			File extractedFile = new File(ZIP_OUTPUT_DIR + File.separator + basename);
			String extractedPath = extractedFile.getAbsolutePath();

			// create all non existing directories
			// 
			// new File(extractedFile.getParent()).mkdirs();

			FileOutputStream fos = new FileOutputStream(extractedFile);
			int len;
			while ((len = zis.read(buffer)) > 0) {
			    fos.write(buffer, 0, len);
			}
			fos.close();
			// System.out.println("ROL3: created new file " + extractedPath + " |" + zeName + "|");

			if (multipleFileFlag) {
			    // if needed invocate populator on the fileslist and delete the extracted files
			    //
			    if (fileslist.size() >= nafPopulator.batchSize) {
				RunSystemOnList(fileslist, nafPopulator.disabledItems, nafPopulator.recursion);
				fileslist.clear();
			    }
			    // add the file to the fileslist
			    //
			    fileslist.addLast(extractedFile);
			} else {
			    // invocate the populator of the extracted file
			    //
			    analyzePathAndRunSystem(extractedPath, nafPopulator.disabledItems, nafPopulator.recursion);
			}
		    }
		    // close entry and get a new one
		    zis.closeEntry();
		    ze = zis.getNextEntry();
		}

		// if needed invocate populator on the fileslist and delete the extracted files
		//
		if (multipleFileFlag && (fileslist.size() > 0)) {
		    RunSystemOnList(fileslist, nafPopulator.disabledItems, nafPopulator.recursion);
		    fileslist.clear();
		}
		
		// close entry and zip
		zis.closeEntry();
		zis.close();

		
	    } else if (nafPopulator.TInFile) {
		/* 
		   input is a compressed tar archive containing NAF files to be processed
		*/
		String TAR_OUTPUT_DIR = "/tmp/nafPopulatorTarOutDir";
		byte[] buffer = new byte[1024];
		LinkedList<File> fileslist = new LinkedList<File>();
		boolean multipleFileFlag = (nafPopulator.batchSize > 1);

		// create output directory is not exists
		File tgzDir = new File(TAR_OUTPUT_DIR);
		if (!tgzDir.exists()) {
		    tgzDir.mkdir();
		}

		// get the tgz file content
		TarArchiveInputStream is = new TarArchiveInputStream(new GzipCompressorInputStream(new FileInputStream(nafPopulator.INpath)));

		// iterate over tgz file list entry
		TarArchiveEntry te = (TarArchiveEntry)is.getNextEntry();
		while (te != null) {

		    // if it is a directory, then skip it, else copy the file contents
		    //
		    if (te.isDirectory()) {
			/*
			  String teName = te.getName();
			  File extractedDir = new File(TAR_OUTPUT_DIR + File.separator + teName);
			  extractedDir.mkdirs();
			  System.out.println("ROL2: created new dir " + extractedDir.getAbsoluteFile());
			*/
		    } else {

			String teName = te.getName();

			// just use the basename of the file
			File tmpFile = new File(TAR_OUTPUT_DIR + File.separator + teName);
			String basename = tmpFile.getName();

			File extractedFile = new File(TAR_OUTPUT_DIR + File.separator + basename);
			String extractedPath = extractedFile.getAbsolutePath();

			// create all non existing directories
			// 
			// new File(extractedFile.getParent()).mkdirs();

			OutputStream outputFileStream = new FileOutputStream(extractedFile); 
			IOUtils.copy(is, outputFileStream);
			outputFileStream.close();
			// System.out.println("ROL2: created new file " + extractedPath + " |" + teName + "|");

			if (multipleFileFlag) {
			    // if needed invocate populator on the fileslist and delete the extracted files
			    //
			    if (fileslist.size() >= nafPopulator.batchSize) {
				RunSystemOnList(fileslist, nafPopulator.disabledItems, nafPopulator.recursion);
				fileslist.clear();
			    }
			    // add the file to the fileslist
			    //
			    fileslist.addLast(extractedFile);
			} else {
			    // invocate the populator of the extracted file
			    //
			    analyzePathAndRunSystem(extractedPath, nafPopulator.disabledItems, nafPopulator.recursion);
			}
		    }
		    // get a new entry
		    te = (TarArchiveEntry)is.getNextEntry();
		}

		// if needed invocate populator on the fileslist and delete the extracted files
		//
		if (multipleFileFlag && (fileslist.size() > 0)) {
		    RunSystemOnList(fileslist, nafPopulator.disabledItems, nafPopulator.recursion);
		    fileslist.clear();
		}
		
		// close the tgz
		is.close();
		
	    } else {
		/* 
		   input is either a NAF file or a NAF directory
		*/
		analyzePathAndRunSystem(nafPopulator.INpath, nafPopulator.disabledItems, nafPopulator.recursion);
	    }
        } catch(Exception e) {
            e.printStackTrace();
            nafPopulator.logger.error(nafPopulator.INpath + " Processing phase: file discarded!\n");
        }
        nafPopulator.JobFinished=true;
    }

    private  void RunSystemOnList(LinkedList<File> fileslist, String disabledItems, boolean rec)
            throws JAXBException, IOException, InstantiationException, IllegalAccessException,
            NoSuchMethodException, SecurityException, ClassNotFoundException, InterruptedException {
        Hashtable<String, KSPresentation> mentions = new Hashtable<String, KSPresentation>();
	boolean submittedFlag = false;
        for(File filePath:fileslist){
            if (filePath.exists() && filePath.isDirectory()) {
                
                File[] listOfFiles = filePath.listFiles();
                
                for (int i = 0; i < listOfFiles.length; i++) {
                    if (listOfFiles[i].exists() && listOfFiles[i].isFile()) {
                        // System.err.println(i + ") working with: " + listOfFiles[i].getName());
                        // out.append("\n" + i + "=" + listOfFiles[i].getName() + "\n");
                        runClass(listOfFiles[i].getPath(), disabledItems,mentions);
                    } else if (listOfFiles[i].exists() && listOfFiles[i].isDirectory()) {
                        analyzePathAndRunSystem(listOfFiles[i].getPath(), disabledItems, rec);
                    }
                    nafPopulator.out.flush();
                    //this is bug applied once it should be i%mod nafPopulator.batchSize==0
                   submittedFlag = checkAddOrSubmit(mentions);

                }
              
                if ((nafPopulator.batchSize == -1) && (! submittedFlag)) {
                    addAndFreeMemory(mentions);
                }
            } else if (filePath.exists() && filePath.isFile()) {
                
                // out.append(filePath.getPath() + "\n");
                runClass(filePath.getPath(), disabledItems,mentions);
                submittedFlag = checkAddOrSubmit(mentions);
            }
        }
        if(! submittedFlag){
            addAndFreeMemory(mentions);
        }
        
	nafPopulator.out.flush();
	if (nafPopulator.printToFile && (nafPopulator.mentionFile != null)) {
	    nafPopulator.mentionFile.flush();
	}
    }

    /*
      return true if mentions have been submitted on the queue
    */
    boolean checkAddOrSubmit(Hashtable<String, KSPresentation> mentions) throws InterruptedException{
        if (((mentions.size() % nafPopulator.batchSize) == 0) && (nafPopulator.batchSize != -1)) {
            addAndFreeMemory(mentions);
	    return true;
        } else {
	    return false;
	}
    }

    void addAndFreeMemory(Hashtable<String, KSPresentation> mentions) throws InterruptedException{
        Producer.queue.put(mentions);
        // empty the heap memory
        mentions = new Hashtable<String, KSPresentation>();
        System.gc();
        Runtime.getRuntime().gc();
    }
    
    private  void analyzePathAndRunSystem(String path, String disabledItems, boolean rec)
            throws JAXBException, IOException, InstantiationException, IllegalAccessException,
            NoSuchMethodException, SecurityException, ClassNotFoundException, InterruptedException {
        File filePath = new File(path);
        if (filePath.exists()) {
            
            if (filePath.exists() && filePath.isDirectory()) {
                // create report file in the same directory of the input file path.
                File[] listOfFiles = filePath.listFiles();
                Hashtable<String, KSPresentation> mentions = new Hashtable<String, KSPresentation>();
                for (int i = 0; i < listOfFiles.length; i++) {
                    if (listOfFiles[i].exists() && listOfFiles[i].isFile()) {
                        // System.err.println(i + ") working with: " + listOfFiles[i].getName());
                        // out.append("\n" + i + "=" + listOfFiles[i].getName() + "\n");
                        runClass(listOfFiles[i].getPath(), disabledItems,mentions);
                    } else if (listOfFiles[i].exists() && listOfFiles[i].isDirectory()) {
                        analyzePathAndRunSystem(listOfFiles[i].getPath(), disabledItems, rec);
                    }
                    //this is bug applied once it should be i%mod nafPopulator.batchSize==0
                    if (nafPopulator.batchSize != -1&&mentions.size() % nafPopulator.batchSize==0 ) {
                        // submit the collected data to KS.
                      /*  if (!nafPopulator.printToFile) {
                            submitCollectedData();
                        } else {
                            appendCollectedDataToFile();
                        }*/
                        
                        Producer.queue.put(mentions);
                        // empty the heap memory
                        mentions = new Hashtable<String, KSPresentation>();
                        System.gc();
                        Runtime.getRuntime().gc();
                    }

                }
                if(mentions.size()>0){
                    Producer.queue.put(mentions);
                    // empty the heap memory
                    mentions = new Hashtable<String, KSPresentation>();
                    System.gc();
                    Runtime.getRuntime().gc();
                }
                //TODO if batchsize ==-1 so submit all once?! check it
                if (nafPopulator.batchSize == -1) {
                    // submit the collected data to KS then it should finish as no other files
                  /*  if (!nafPopulator.printToFile) {
                        submitCollectedData();
                    } else {
                        appendCollectedDataToFile();
                    }*/
                    Producer.queue.put(mentions);
                    // empty the heap memory
                    mentions = new Hashtable<String, KSPresentation>();
                    System.gc();
                    Runtime.getRuntime().gc();
                }
            } else if (filePath.exists() && filePath.isFile()) {
               
                // out.append(filePath.getPath() + "\n");
                Hashtable<String, KSPresentation> mentions = new Hashtable<String, KSPresentation>();
                runClass(filePath.getPath(), disabledItems,mentions);
                /*if (!nafPopulator.printToFile) {
                    submitCollectedData();
                } else {
                    appendCollectedDataToFile();
                }*/
                Producer.queue.put(mentions);
                // empty the heap memory
                mentions = new Hashtable<String, KSPresentation>();
                System.gc();
                Runtime.getRuntime().gc();
            }
            if (nafPopulator.printToFile && 
		(nafPopulator.mentionFile != null)) {
                nafPopulator.mentionFile.flush();
            }
        } else {
            System.err.println("Path not exist!" + filePath.getPath());

        }
    }

    
    
    public  void runClass(String path, String disabledItems, Hashtable<String, KSPresentation> mentions) throws InstantiationException,
            IllegalAccessException, NoSuchMethodException, SecurityException,
            ClassNotFoundException, IOException {

        System.out.println(path); // TODO
        String className = "eu.fbk.knowledgestore.populator.naf.processNAF";
        Class clazz = Class.forName(className);
        Class[] parameters = new Class[] { String.class, Writer.class, String.class, boolean.class };
        Method method = clazz.getMethod("init", parameters);
        Object obj = clazz.newInstance();
        try {
            KSPresentation as = (KSPresentation) method.invoke(obj, path, nafPopulator.out, disabledItems,
                    nafPopulator.store_partial_info);
            if (as != null) {
                mentions.put(path, as);
            } else {
                nafPopulator.out.append(path + " null is returned from processNAF procedure!");
            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            nafPopulator.logger.error(path + " Processing phase: file discarded!\n");
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            nafPopulator.logger.error(path + " Processing phase: file discarded!\n");
        } catch (InvocationTargetException e) {
            e.printStackTrace();
            nafPopulator.logger.error(path + " Processing phase: file discarded!\n");
        }

    }

}
