package org.apache.spark.dca;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.apache.spark.dca.builder.AdaptiveTunerBuilder;
import org.apache.spark.executor.Executor;
import org.apache.spark.executor.Executor.TaskRunner;

public class SelfAdaptiveThreadPoolExecutor extends MyThreadPoolExecutor {

	private final static Logger log = Logger.getLogger("SelfAdaptiveThreadPoolExecutor");

	private AtomicInteger initialMaximumPoolSize = new AtomicInteger(0);
	private AdaptiveTuner tuner;
	
	private Boolean isUpdateSchedulerEnabled = false;

	public SelfAdaptiveThreadPoolExecutor(int corePoolSize, int maximumPoolSize, int changedCores, long keepAliveTime, TimeUnit unit,
			BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, int adaptiveThreadPool, Executor executor) {
		super(changedCores, changedCores, keepAliveTime, unit, workQueue, threadFactory, executor);
		
		if(changedCores != 0) {
			AdaptiveTunerBuilder tunerBuilder = new AdaptiveTunerBuilder();
			this.tuner = tunerBuilder.build(adaptiveThreadPool, this, maximumPoolSize);
		}
	}

	@Override
	protected void beforeExecute(Thread t, Runnable r) {
		tuner.beforeExecute(t, r);
		super.beforeExecute(t, r);
	}

	@Override
	protected void afterExecute(Runnable r, Throwable t) {
		tuner.afterExecute(r, t);
		super.afterExecute(r, t);
	}

	@Override
	public void execute(Runnable command) {
		tuner.execute(command);
		if (command instanceof TaskRunner) {
			TaskRunner taskRunner = (TaskRunner) command;

			// If the stage has changed, reset the threadpool to its initial state.
			if (getCurrentStage() != taskRunner.getStageId()) {
				tuner.reset();
			}
		}
		super.execute(command);
	}



	public int getInitialMaximumPoolSize() {
		return initialMaximumPoolSize.get();
	}

	public AdaptiveTuner getTuner() {
		return tuner;
	}

	@Override
	public void shutdown() {
		// TODO Auto-generated method stub
		tuner.shutdown();
		super.shutdown();
	}

	public void setInitialMaximumPoolSize(int val) {
		this.initialMaximumPoolSize.set(val);
	}
}
