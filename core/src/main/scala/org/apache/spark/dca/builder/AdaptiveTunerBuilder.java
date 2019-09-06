package org.apache.spark.dca.builder;

import org.apache.log4j.Logger;
import org.apache.spark.dca.AdaptiveTuner;
import org.apache.spark.dca.AscendingAdaptiveTuner;
import org.apache.spark.dca.DecendingAdaptiveTuner;
import org.apache.spark.dca.DecendingBlockingAdaptiveTuner;
import org.apache.spark.dca.FixedAdaptiveTuner;
import org.apache.spark.dca.NoAdaptiveTuner;
import org.apache.spark.dca.SelfAdaptiveThreadPoolExecutor;
import org.apache.spark.dca.strace.NoStraceAdaptiveTunerBuildDirector;

public class AdaptiveTunerBuilder {
	private final static Logger log = Logger.getLogger("AdaptiveTunerBuilder");
	
	public AdaptiveTuner build(int adaptiveThreadPool, SelfAdaptiveThreadPoolExecutor threadPool, int maximumPoolSize) {
		log.info(String.format("Asked to build type [%s] adaptive threadpool with %s threads", adaptiveThreadPool,
				maximumPoolSize));
		if (adaptiveThreadPool == 1)
			return new DecendingAdaptiveTuner(threadPool, maximumPoolSize);
		else if (adaptiveThreadPool == 2)
			return new DecendingBlockingAdaptiveTuner(threadPool, maximumPoolSize);
		else if (adaptiveThreadPool == 11)
			return new DefaultAdaptiveTunerBuildDirector(new AscendingAdaptiveTuner()).construct(threadPool, maximumPoolSize);
		else if (adaptiveThreadPool == 12)
			return new PythonStdinStraceAdaptiveTunerBuildDirector(new AscendingAdaptiveTuner()).construct(threadPool, maximumPoolSize);
		else if (adaptiveThreadPool == 13)
			return new FileStraceAdaptiveTunerBuildDirector(new AscendingAdaptiveTuner()).construct(threadPool, maximumPoolSize);
		else if (adaptiveThreadPool == 14)
			return new PythonFileStraceAdaptiveTunerBuildDirector(new AscendingAdaptiveTuner()).construct(threadPool, maximumPoolSize);
		else if (adaptiveThreadPool == 15)
			return new NoStraceAdaptiveTunerBuildDirector(new AscendingAdaptiveTuner()).construct(threadPool, maximumPoolSize);
//		else if (adaptiveThreadPool == 21)
//			return new NoStraceAdaptiveTunerBuildDirector(new FixedAdaptiveTuner(threadPool)).construct(threadPool, maximumPoolSize);
//		else if (adaptiveThreadPool == 13)
//			return new AscendingAdaptiveBlockingTuner(threadPool, maximumPoolSize);
//		else if (adaptiveThreadPool == 99)
//			return new FileStraceAdaptiveTunerBuildDirector(new NoAdaptiveTuner()).construct(threadPool, maximumPoolSize);
//			return new NoAdaptiveTuner(threadPool, maximumPoolSize);
		else
			return null;
	}
}
