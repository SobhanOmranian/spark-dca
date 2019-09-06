package org.apache.spark.dca;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.apache.spark.executor.Executor.TaskRunner;

public class AscendingAdaptiveTuner extends AdaptiveTuner {

	private final static Logger log = Logger.getLogger("AscendingAdaptiveTuner");

	private AtomicBoolean isMaxReached = new AtomicBoolean(false);

	private int minThreadNum = 2;

	public AscendingAdaptiveTuner() {
	}

	public void initialise() {
		log.info("Initializing...");
		super.initialise();

		setLastThreadPoolSize(minThreadNum);
		getThreadPool().setThreadSize(minThreadNum);
	}

	@Override
	public int tune(TaskRunner taskRunner, int threadNum) {
		try {
			long startTime = System.currentTimeMillis();
			super.tune(taskRunner, threadNum);

			// int currentPoolSize = getLastThreadPoolSize();
			int currentMaximumPoolSize = getThreadPool().getMaximumPoolSize();

			// Stop if it is getting worse.
			if (getLastThreadPoolSize() != currentMaximumPoolSize) {
				ArrayList<Float> currentMonitoringResult = monitoringResults.get(currentMaximumPoolSize);
				ArrayList<Float> previousMonitoringResult = monitoringResults.get(getLastThreadPoolSize());

//				epoll normalisedByTotalTaskThroughputFromSampling
				int index = 4;

				float currentValue = currentMonitoringResult.get(index);
				float previousValue = previousMonitoringResult.get(index);
				
				Boolean shouldRevert = true;

				log.info(String.format(
						"[Skip]: currentTotalTaskThroughputFromSampling[%s]: %s, previousTotalTaskThroughputFromSampling[%s]: %s",
						currentMaximumPoolSize, currentValue, getLastThreadPoolSize(), previousValue));

				if (currentValue > previousValue) {
					if (shouldRevert) {
						log.info(String.format(
								"[Skip]: epoll_wait has increased when threadNum[%s], reverting back to threadNum[%s] and stopping...",
								currentMaximumPoolSize, getLastThreadPoolSize()));
						// log.debug(String.format("[Skip]: Throughput has declined when threadNum[%s],
						// reverting back to threadNum[%s] and stopping...", currentMaximumPoolSize,
						// getLastThreadPoolSize()));
						setTuningFinished(true);
						getThreadPool().setThreadSize(getLastThreadPoolSize());
						report(taskRunner.getStageId());
						return getLastThreadPoolSize();
					}
				}
			}

			int minThreadNumber = minThreadNum;
			int maxThreadNumber = getThreadPool().getInitialMaximumPoolSize();

			int coreSize = getThreadPool().getInitialMaximumPoolSize();

			if (currentMaximumPoolSize == minThreadNumber) {
				// We have reached the bottom. Double the thread number.
				int newValue = (int) Math.floor(currentMaximumPoolSize * 2);
				coreSize = Math.min(maxThreadNumber, newValue);
				isMaxReached.set(false);
			} else if (currentMaximumPoolSize == maxThreadNumber) {
				// We have reached the top. Half the thread number.
				int newValue = (int) Math.ceil(currentMaximumPoolSize / 2);
				coreSize = Math.max(minThreadNumber, newValue);
				isMaxReached.set(true);

				// We have reached the top; stop.
				log.info(String.format("We have reached the max (%s) number of threads, so reporting...",
						currentMaximumPoolSize));
				int optimal = report(taskRunner.getStageId());
				getThreadPool().setThreadSize(optimal);
				setTuningFinished(true);
				return optimal;

			} else {
				// We are between, go up or down.
				if (!isMaxReached.get()) {
					// Go up
					int newValue = (int) Math.floor(currentMaximumPoolSize * 2);
					coreSize = Math.min(maxThreadNumber, newValue);
				} else {
					// Go down
					int newValue = (int) Math.ceil(currentMaximumPoolSize / 2);
					coreSize = Math.max(minThreadNumber, newValue);
				}

			}
			setLastThreadPoolSize(getThreadPool().getMaximumPoolSize());
			getThreadPool().setThreadSize(coreSize);
			long stopTime = System.currentTimeMillis();
			long elapsedExecutionTime = stopTime - startTime;
			log.info(String.format("[Tune]: tune duration: %s ms", elapsedExecutionTime));
			return coreSize;
		} catch (Exception ex) {
			// TODO Auto-generated catch block
			ex.printStackTrace();
			log.debug("[Exception] => Exception while tuning: " + ex.getMessage());
		}

		return 0;
	}

	@Override
	public void afterExecute(Runnable r, Throwable t) {
		synchronized (this) {
			super.afterExecute(r, t);

			if (r instanceof TaskRunner && !isTuningFinished()) {
				TaskRunner taskRunner = (TaskRunner) r;

				log.info(String.format("FinishedTaskNum: %s, SubmitedTaskNum: %s, TuningAt: %s finished tasks.",
						getFinishedTasksNum(), getSubmittedTasksNum(), getThreadPool().getMaximumPoolSize())); 
				
				int multipleTimesThreadThreshold = 8;
				// Here we check if we should tune.
				// For number of threads less equal to [multipleTimesThreadThreshold], we check
				// multiple (2) times. This is for the cases where interval time is really
				// short.
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

		// If we are running num tasks equal to maximum pool size, start monitoring.
		// For example, if we want monitor(16), then 16 tasks should be running at the
		// same time for start monitoring.
		// We increment [activeTaskCount] because the about-to-start task is not
		// included in
		// the active count.
		int activeTaskCount = getThreadPool().getActiveCount();
		log.debug(String.format("Running %s tasks => maxPoolSize: %s", activeTaskCount,
				getThreadPool().getMaximumPoolSize()));

		// "monitoringStarted" is just a flag to avoid race conditions.
		if (activeTaskCount == getThreadPool().getMaximumPoolSize() && !isMonitoringStarted() && !isTuningFinished()) {
			startMonitoring(activeTaskCount);
			return;
		}
	}

	@Override
	public void reset() {

		getThreadPool().setThreadSize(minThreadNum);
		setLastThreadPoolSize(minThreadNum);
		super.reset();
	}

}
