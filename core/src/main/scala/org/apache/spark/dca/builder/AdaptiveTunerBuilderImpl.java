package org.apache.spark.dca.builder;

import java.util.concurrent.ThreadPoolExecutor;

import org.apache.log4j.Logger;
import org.apache.spark.dca.AdaptiveTuner;
import org.apache.spark.dca.AscendingAdaptiveTuner;
import org.apache.spark.dca.NoAdaptiveTuner;
import org.apache.spark.dca.Sampler;
import org.apache.spark.dca.SelfAdaptiveThreadPoolExecutor;
import org.apache.spark.dca.strace.IStraceReader;

public class AdaptiveTunerBuilderImpl implements IAdaptiveTunerBuilder {
	
	private final static Logger log = Logger.getLogger("AdaptiveTunerBuilderImpl");
	
	private AdaptiveTuner tunerTmp;
	
	public AdaptiveTunerBuilderImpl(AdaptiveTuner tuner) {
//		tunerTmp = new AscendingAdaptiveTuner();
		tunerTmp = tuner;
	}

	@Override
	public AdaptiveTuner build() {
		// TODO Auto-generated method stub
		AdaptiveTuner tuner = tunerTmp;
//		tuner.setThreadPool(tunerTmp.getThreadPool());
//		tuner.threadPool.setMaximumPoolSize(tunerTmp.threadPool.getMaximumPoolSize());
//		tuner.setStraceReader(tunerTmp.getStraceReader());
//		tuner.setSampler(tunerTmp.getSampler());
		
		tuner.initialise();
		return tuner;
	}

	@Override
	public IAdaptiveTunerBuilder setStraceReader(final IStraceReader straceReader) {
		// TODO Auto-generated method stub
		tunerTmp.setStraceReader(straceReader);
		return this;
	}
	
	@Override
	public IAdaptiveTunerBuilder setThreadPool(final SelfAdaptiveThreadPoolExecutor threadPool) {
		// TODO Auto-generated method stub
		tunerTmp.setThreadPool(threadPool);
		return this;
	}
	
	@Override
	public IAdaptiveTunerBuilder setMaximumPoolSize(final int maximumPoolSize) {
		// TODO Auto-generated method stub
		if (tunerTmp.threadPool == null)
			log.error("Thread pool must be initialised before setting the maximum pool size.");
		
		tunerTmp.getThreadPool().setInitialMaximumPoolSize(maximumPoolSize);
		return this;
	}

	@Override
	public IAdaptiveTunerBuilder setSampler(Sampler sampler) {
		tunerTmp.sampler = sampler;
		return this;
	}
	

}
