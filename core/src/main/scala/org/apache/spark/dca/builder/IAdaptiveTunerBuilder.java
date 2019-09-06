package org.apache.spark.dca.builder;

import org.apache.log4j.Logger;
import org.apache.spark.dca.AdaptiveTuner;
import org.apache.spark.dca.Sampler;
import org.apache.spark.dca.SelfAdaptiveThreadPoolExecutor;
import org.apache.spark.dca.strace.IStraceReader;

public interface IAdaptiveTunerBuilder {
//	private final static Logger log = Logger.getLogger("AdaptiveTunerBuilder");
	AdaptiveTuner build();
	IAdaptiveTunerBuilder setStraceReader(final IStraceReader straceReader);
	IAdaptiveTunerBuilder setThreadPool(final SelfAdaptiveThreadPoolExecutor threadPool);
	IAdaptiveTunerBuilder setMaximumPoolSize(final int maximumPoolSize);
	IAdaptiveTunerBuilder setSampler(final Sampler sampler);

}
