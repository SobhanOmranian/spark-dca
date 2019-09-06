package org.apache.spark.dca.strace;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.apache.spark.dca.MyUtils;

public class FileStraceReader implements IStraceReader {
	
	private final static Logger log = Logger.getLogger("FileStraceReader");

	private AtomicInteger currentLineNumberStrace = new AtomicInteger(0);
	private String logPath = "";
	
	public FileStraceReader() {
	}
	
	@Override
	public float getAggregatedEpollWaitTime() {
		log.debug("[Strace] => Reading strace file at location: " + this.getLogPath());
		
		float total = 0;
		
		List<String> lines = MyUtils.readFileInListFromTo(logPath, getCurrentLineNumberStrace());
		if (lines == null) {
			return 0;
		}
		currentLineNumberStrace.addAndGet(lines.size());
		log.debug(String.format("Current IoStat line number is now: %s", currentLineNumberStrace));

		for (String line : lines) {
			total += MyUtils.getTimeFromLine(line);
		}

		return total;
		
	}

	@Override
	public void startMonitoring(int stageId) {
		if (logPath != "") {
			setCurrentLineNumberStrace(MyUtils.readFileInList(logPath).size());
			log.debug(String.format("Starting monitoring strace log at line: %s", getCurrentLineNumberStrace()));
		}
	}
	
	public int getCurrentLineNumberStrace() {
		return currentLineNumberStrace.get();
	}

	public void setCurrentLineNumberStrace(int val) {
		this.currentLineNumberStrace.set(val);
	}

	public String getLogPath() {
		return logPath;
	}

	public void setLogPath(final String logPath) {
		this.logPath = logPath;
	}

	@Override
	public void close() {
		
	}

	@Override
	public Long getOverheadTime() {
		// TODO Auto-generated method stub
		return MyUtils.timeTakenInRead;
	}

}
