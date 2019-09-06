package org.apache.spark.dca;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.apache.spark.dca.builder.AdaptiveTunerBuilder;
import org.apache.spark.dca.strace.NoStraceReader;
import org.apache.spark.executor.Executor;
import org.apache.spark.executor.Executor.TaskRunner;

public class SelfAdaptiveFixedThreadPoolExecutor extends MyThreadPoolExecutor {

	private final static Logger log = Logger.getLogger("SelfAdaptiveFixedThreadPoolExecutor");

	private AtomicInteger initialMaximumPoolSize = new AtomicInteger(0);
	private Tuner tuner;

	public SelfAdaptiveFixedThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
			BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, int adaptiveThreadPool, Executor executor) {
		super(maximumPoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, executor);
		
		this.tuner = new FixedAdaptiveTuner(this);
		tuner.straceReader = new NoStraceReader();
	}
	

	
	@Override
	public void execute(Runnable command) {
		tuner.execute(command);
		super.execute(command);

	}
	
	public void saveDca(int stageId) {
		((FixedAdaptiveTuner)tuner).reportNonIo(stageId);
	}

}
