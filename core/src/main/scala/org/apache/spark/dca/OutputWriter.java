package org.apache.spark.dca;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.log4j.Logger;

public abstract class OutputWriter {
	private final static Logger log = Logger.getLogger("OutputWriter");

	private String outputFileDir = "/home/omranian/results/";
	BufferedWriter bw;
	

	public OutputWriter(String fileName, String header) {
		try {
			String outputFilePath = outputFileDir + fileName;
			log.debug(String.format("Creating output file at: %s", outputFilePath));
			log.debug(String.format("[OutputWriter]: Requested header: %s", header));
			
			File outputFile = new File(outputFilePath);
			FileWriter fw = new FileWriter(outputFile.getAbsoluteFile());
			bw = new BufferedWriter(fw);
			if(bw == null)
				log.error(String.format("[OutputWriter]: BufferedWriter is null!"));
			write(header);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	

	public void write(String s) {
		try {
			bw.write(s);
			bw.newLine();
		} catch (IOException e) {
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
