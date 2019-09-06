package org.apache.spark.dca.builder;

import org.apache.log4j.Logger;
import org.apache.spark.dca.AdaptiveTuner;
import org.apache.spark.dca.Sampler;
import org.apache.spark.dca.SelfAdaptiveThreadPoolExecutor;
import org.apache.spark.dca.Tuner;
import org.apache.spark.dca.strace.FileStraceReader;

public abstract class AdaptiveTunerBuildDirector {
	private final static Logger log = Logger.getLogger("AdaptiveTunerBuildDirector");
	private IAdaptiveTunerBuilder builder;
	
	public AdaptiveTunerBuildDirector(Tuner tuner) {
		this.builder = new AdaptiveTunerBuilderImpl((AdaptiveTuner)tuner);
	}
	
	public AdaptiveTuner construct(SelfAdaptiveThreadPoolExecutor threadPool, int maximumPoolSize) {

		
		return builder.setThreadPool(threadPool)
				.setSampler(new Sampler(threadPool))
				.setStraceReader(new FileStraceReader())
				.setMaximumPoolSize(maximumPoolSize)
				.build();
	}
	

}
