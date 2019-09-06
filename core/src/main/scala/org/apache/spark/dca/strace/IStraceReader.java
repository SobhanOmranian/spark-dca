package org.apache.spark.dca.strace;

public interface IStraceReader {
	float getAggregatedEpollWaitTime();
	void startMonitoring(int stageId);
	void setLogPath(final String path);
	
	Long getOverheadTime();
	
	void close();
}
