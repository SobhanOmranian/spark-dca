package org.apache.spark.dca;

import org.apache.log4j.Logger;

public class TaskFinishOutputWriter extends OutputWriter {
	private final static Logger log = Logger.getLogger("TaskFinishOutputWriter");
	
	private static String header = String.format(
			"executorId, taskId, startTime, finishTime, startCores, finishCores");

	public TaskFinishOutputWriter(String fileName) {
		super(fileName+".taskfinish", header);	
	}

	
	@Override
	public String getFileHeader() {
		return header;
	}
}
