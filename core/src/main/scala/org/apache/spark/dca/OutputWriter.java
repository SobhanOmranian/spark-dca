package org.apache.spark.dca;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.log4j.Logger;

public abstract class OutputWriter {
	private final static Logger log = Logger.getLogger("OutputWriter");
	String RESULT_HOME = System.getenv("RESULT_HOME");
//	if (RESULT_HOME == null)  {
//		RESULT_HOME = "/home/omranian/results/";
//	}
	private String outputFileDir = RESULT_HOME;
	BufferedWriter bw;
	

	public OutputWriter(String fileName, String header) {
		try {
			if (RESULT_HOME == null) {
				RESULT_HOME = "/home/omranian/results/";
			}
			outputFileDir = RESULT_HOME;
			String outputFilePath = outputFileDir + "/" +  fileName;
			log.info(String.format("Creating output file at: %s", outputFilePath));
			log.info(String.format("[OutputWriter]: Requested header: %s", header));
			
			File outputFile = new File(outputFilePath);
			FileWriter fw = new FileWriter(outputFile.getAbsoluteFile());
			bw = new BufferedWriter(fw);
			if(bw == null)
				log.error(String.format("[OutputWriter]: BufferedWriter is null!"));
			write(header);
		} catch (IOException e) {
			log.error(String.format("IoException in OutputWriter: %s", e.getMessage()));
			e.printStackTrace();
		}
		catch (Exception e) {
			log.error(String.format("Exception in OutputWriter: %s", e.getMessage()));
			e.printStackTrace();
		}
	}
	
	

	public void write(String s) {
		try {
			log.info(String.format("[OutputWriter]: Writing line: %s", s));
			bw.write(s);
			bw.newLine();
		} catch (IOException e) {
			log.error(String.format("IoException while writing: %s", e.getMessage()));
			e.printStackTrace();
		}
		catch (Exception e) {
			log.error(String.format("Exception while writing: %s", e.getMessage()));
			e.printStackTrace();
		}
	}
	
	public void close() {
		try {
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public String getFileHeader() {
		return "default header";
	}
}
