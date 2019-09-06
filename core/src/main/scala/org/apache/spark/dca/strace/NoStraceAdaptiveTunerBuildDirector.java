package org.apache.spark.dca.strace;

import org.apache.log4j.Logger;
import org.apache.spark.dca.AdaptiveTuner;
import org.apache.spark.dca.SelfAdaptiveThreadPoolExecutor;
import org.apache.spark.dca.builder.AdaptiveTunerBuildDirector;

public class NoStraceAdaptiveTunerBuildDirector extends AdaptiveTunerBuildDirector {

	private final static Logger log = Logger.getLogger("AdaptiveTunerBuildDirector");
	public NoStraceAdaptiveTunerBuildDirector(AdaptiveTuner tuner) {
		super(tuner);
	}

	@Override
	public AdaptiveTuner construct(SelfAdaptiveThreadPoolExecutor threadPool, int maximumPoolSize) {
		AdaptiveTuner tuner = super.construct(threadPool, maximumPoolSize);
			
		tuner.setStraceReader(new NoStraceReader());
		return tuner;
	}
	
	
}
