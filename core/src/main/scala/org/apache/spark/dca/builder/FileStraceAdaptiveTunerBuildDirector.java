package org.apache.spark.dca.builder;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;
import org.apache.spark.dca.AdaptiveTuner;
import org.apache.spark.dca.SelfAdaptiveThreadPoolExecutor;
import org.apache.spark.dca.Tuner;
import org.apache.spark.dca.strace.FileStraceReader;
import org.apache.spark.dca.strace.NoStraceReader;

public class FileStraceAdaptiveTunerBuildDirector extends AdaptiveTunerBuildDirector {

	private final static Logger log = Logger.getLogger("FileStraceAdaptiveTunerBuildDirector");

	public FileStraceAdaptiveTunerBuildDirector(Tuner tuner) {
		super(tuner);
	}

	@Override
	public AdaptiveTuner construct(SelfAdaptiveThreadPoolExecutor threadPool, int maximumPoolSize) {
		AdaptiveTuner tuner = super.construct(threadPool, maximumPoolSize);

		int pid = threadPool.executor.getPid();
		log.debug(String.format("[STRACE]: Attaching File Strace to my PID: %s", pid));
		tuner.setStraceReader(new FileStraceReader());
		
		return tuner;
	}


}
