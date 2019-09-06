package org.apache.spark.dca.strace;

public class NoStraceReader implements IStraceReader {
	
//	private final static Logger log = Logger.getLogger("NoStraceReader");

	
	public NoStraceReader() {
	}
	
	@Override
	public float getAggregatedEpollWaitTime() {
		return 0;
	}

	@Override
	public void startMonitoring(int stageId) {
		// Do nothing
	}
	

	@Override
	public void close() {		
	}

	@Override
	public Long getOverheadTime() {
		return 0L;
	}

	@Override
	public void setLogPath(String path) {
	}

}
