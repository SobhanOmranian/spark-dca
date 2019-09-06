package org.apache.spark.dca.builder;

import org.apache.log4j.Logger;
import org.apache.spark.dca.AdaptiveTuner;
import org.apache.spark.dca.SelfAdaptiveThreadPoolExecutor;
import org.apache.spark.dca.strace.PythonFileStraceReader;
import org.apache.spark.dca.strace.PythonStdinStraceReader;

public class PythonFileStraceAdaptiveTunerBuildDirector extends AdaptiveTunerBuildDirector {

	private final static Logger log = Logger.getLogger("PythonStraceAdaptiveTunerBuildDirector");
	public PythonFileStraceAdaptiveTunerBuildDirector(AdaptiveTuner tuner) {
		super(tuner);
	}

	@Override
	public AdaptiveTuner construct(SelfAdaptiveThreadPoolExecutor threadPool, int maximumPoolSize) {
		AdaptiveTuner tuner = super.construct(threadPool, maximumPoolSize);
		
		
		int pid = threadPool.executor.getPid();
		log.debug(String.format("[STRACE]: Attaching Strace to my PID: %s", pid));
		threadPool.executor.attachStraceToPythonFileParser(pid);
		
		
		tuner.setStraceReader(new PythonFileStraceReader(tuner));
		return tuner;
	}
	
	
}
