package org.apache.spark.dca;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.log4j.Logger;

public class DcaResultOutputWriter extends OutputWriter {
	private final static Logger log = Logger.getLogger("DcaResultOutputWriter");

	
	private static String header = String.format(
			"executorId,stage,cores,totallEpollWait,normalisedByThreadNum,normalisedByTime,normalisedByBoth,normalisedByTotalTaskThroughputFromSampling,diskThroughput,avgTaskThroughput,totalTaskThroughput,totalTaskReadThroughputFromSampling,totalTaskWriteThroughputFromSampling,totalTaskBothThroughputFromSampling,avgTaskThroughputFromSampling,totalTaskGcPercentageFromSampling,totalTime,totalFileReadTime,selection");

	public DcaResultOutputWriter(String fileName) {
		super(fileName+".dca", header);	
	}

	
	@Override
	public String getFileHeader() {
		return header;
	}
}
