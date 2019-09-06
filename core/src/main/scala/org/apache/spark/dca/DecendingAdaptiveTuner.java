package org.apache.spark.dca;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.apache.spark.executor.Executor.TaskRunner;

public class DecendingAdaptiveTuner extends AdaptiveTuner {

	private final static Logger log = Logger.getLogger("DecendingAdaptiveTuner");

	private AtomicBoolean isMinReached = new AtomicBoolean(false);

	public DecendingAdaptiveTuner(SelfAdaptiveThreadPoolExecutor threadPool, int maximumPoolSize) {
		threadPool.setInitialMaximumPoolSize(maximumPoolSize);
	}

	@Override
	public int tune(TaskRunner taskRunner, int threadNum) {
		try {
			long startTime = System.currentTimeMillis();
			super.tune(taskRunner, threadNum);
			
			
			int minThreadNumber = 2;
			int maxThreadNumber = getThreadPool().getInitialMaximumPoolSize();

			int coreSize = getThreadPool().getInitialMaximumPoolSize();

			// Should we go up or go down?
			// Currently we stop if we reach the bottom;

			int currentMaximumPoolSize = getThreadPool().getMaximumPoolSize();
			if (currentMaximumPoolSize == minThreadNumber) {
				// We have reached the bottom. Double the thread number.
				int newValue = (int) Math.floor(currentMaximumPoolSize * 2);
				coreSize = Math.min(maxThreadNumber, newValue);
				isMinReached.set(true);
				// We have reached the bottom; stop.
				log.debug(String.format("We have reached min number of threads, so reporting..."));
				int optimal = report(taskRunner.getStageId());
				getThreadPool().setThreadSize(optimal);
				setTuningFinished(true);
				return optimal;
			} else if (currentMaximumPoolSize == maxThreadNumber) {
				// We have reached the top. Half the thread number.
				int newValue = (int) Math.ceil(currentMaximumPoolSize / 2);
				coreSize = Math.max(minThreadNumber, newValue);
				isMinReached.set(false);
			} else {
				// We are between, go up or down.
				if (isMinReached.get()) {
					// Go up
					int newValue = (int) Math.floor(currentMaximumPoolSize * 2);
					coreSize = Math.min(maxThreadNumber, newValue);
				} else {
					// Go down
					int newValue = (int) Math.ceil(currentMaximumPoolSize / 2);
					coreSize = Math.max(minThreadNumber, newValue);
				}
			}

			if (currentMaximumPoolSize == minThreadNumber) {
				// We have reached the bottom; stop.
			}

			getThreadPool().setThreadSize(coreSize);

			long stopTime = System.currentTimeMillis();
			long elapsedExecutionTime = stopTime - startTime;
			log.debug(String.format("[Tune]: tune duration: %s ms", elapsedExecutionTime));
			return coreSize;
		} catch (Exception ex) {
			ex.printStackTrace();
			log.debug("[Exception] => exception while tuning: " + ex.getMessage());
		}

		return -1;
	}

	@Override
	public void afterExecute(Runnable r, Throwable t) {
		synchronized (this) {
			super.afterExecute(r, t);

			if (r instanceof TaskRunner && !isTuningFinished()) {
				TaskRunner taskRunner = (TaskRunner) r;

				log.debug(String.format("FinishedTaskNum: %s, SubmitedTaskNum: %s, TuningAt: %s finished tasks.",
						getFinishedTasksNum(), getSubmittedTasksNum(), getThreadPool().getMaximumPoolSize())); // maximum
																											// pull
																											// size

				// Long bytesRead = 0L;
				// boolean checkTuning = isTuning.get();

				// This is for the tune(32) if we have 32 cores. Because the first task that
				// finishes should trigger tuning.
				// For others (e.g., tune(16) or tune(8)), the rest of the code handles it.
//				if (getThreadPool().getMaximumPoolSize() == getThreadPool().getInitialMaximumPoolSize()) {
//					if (getFinishedTasksNum() == 1) {
//						tune(taskRunner);
//					}
//					return;
//				}

				// If we are running num tasks equal to maximum pool size, start monitoring.
				// For example, if we want monitor(16), then 16 tasks should be running at the
				// same time for start monitoring.
				// We decrement [activeTaskCount] because the finished task is also included in
				// the active count.
				int activeTaskCount = getThreadPool().getActiveCount() - 1;
				log.debug(String.format("Running %s tasks => maxPoolSize: %s", activeTaskCount,
						getThreadPool().getMaximumPoolSize()));

				// "monitoringStarted" is just a flag to avoid race conditions.
				if (activeTaskCount == getThreadPool().getMaximumPoolSize() && !isMonitoringStarted()) {
					startMonitoring(activeTaskCount);
					return;
				}

				int multipleTimesThreadThreshold = 8;
				// Here we check if we should tune.
				// For number of threads less equal to [multipleTimesThreadThreshold], we check
				// multiple (2) times.
				if ((getFinishedTasksNum() != 0) && getFinishedTasksNum() % getThreadPool().getMaximumPoolSize() == 0) {
					if (getThreadPool().getMaximumPoolSize() <= multipleTimesThreadThreshold
							&& getFinishedTasksNum() / getThreadPool().getMaximumPoolSize() == 1) {
						log.debug(String.format("[Extra]: poolSize: %s, finishedTaskNum: %s, Skip tunning...",
								getThreadPool().getMaximumPoolSize(), getFinishedTasksNum()));
					} else {
						log.debug(String.format("[Extra]: poolSize: %s, finishedTaskNum: %s, Time to tune",
								getThreadPool().getMaximumPoolSize(), getFinishedTasksNum()));
						tune(taskRunner, getThreadPool().getCorePoolSize());
						// We set monitoring started to false, so that we can start monitoring for the
						// next interval.
						setMonitoringStarted(false);
					}
				}

			}
		}
	}

	@Override
	public void beforeExecute(Thread t, Runnable r) {
		super.beforeExecute(t, r);
	}

	@Override
	public void reset() {

		getThreadPool().setThreadSize(getThreadPool().getInitialMaximumPoolSize());
		super.reset();
	}

}
