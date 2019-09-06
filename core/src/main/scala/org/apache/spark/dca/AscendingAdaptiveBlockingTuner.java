package org.apache.spark.dca;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.apache.spark.executor.Executor.TaskRunner;

// This is a blocking tuner which means it will wait for [x] tasks to finish completely in interval_x.
public class AscendingAdaptiveBlockingTuner extends AdaptiveTuner {

	private final static Logger log = Logger.getLogger("AscendingAdaptiveBlockingTuner");

	private AtomicBoolean isMaxReached = new AtomicBoolean(false);

	private int minThreadNum = 2;

	public AscendingAdaptiveBlockingTuner() {
		super();
		setLastThreadPoolSize(minThreadNum);
		threadPool.setThreadSize(minThreadNum);
//		threadPool.setInitialMaximumPoolSize(maximumPoolSize);
	}

	@Override
	public int tune(TaskRunner taskRunner, int threadNum) {
		try {
			long startTime = System.currentTimeMillis();
			super.tune(taskRunner, threadNum);
			int minThreadNumber = minThreadNum;
			int maxThreadNumber = getThreadPool().getInitialMaximumPoolSize();

			int currentPoolSize = lastThreadPoolSize.get();

			int coreSize = getThreadPool().getInitialMaximumPoolSize();

			int currentMaximumPoolSize = threadPool.getMaximumPoolSize();
			if (currentPoolSize == minThreadNumber) {
				// We have reached the bottom. Double the thread number.
				int newValue = (int) Math.floor(currentPoolSize * 2);
				coreSize = Math.min(maxThreadNumber, newValue);
				isMaxReached.set(false);
			} else if (currentPoolSize == maxThreadNumber) {
				// We have reached the top. Half the thread number.
				int newValue = (int) Math.ceil(currentPoolSize / 2);
				coreSize = Math.max(minThreadNumber, newValue);
				isMaxReached.set(true);

				// We have reached the top; stop.
				log.debug(String.format("We have reached the max (%s) number of threads, so reporting...",
						currentMaximumPoolSize));
				int optimal = report(taskRunner.getStageId());
				threadPool.setThreadSize(optimal);
				setTuningFinished(true);
				return optimal;

			} else {
				// We are between, go up or down.
				if (!isMaxReached.get()) {
					// Go up
					int newValue = (int) Math.floor(currentPoolSize * 2);
					coreSize = Math.min(maxThreadNumber, newValue);
				} else {
					// Go down
					int newValue = (int) Math.ceil(currentPoolSize / 2);
					coreSize = Math.max(minThreadNumber, newValue);
				}

			}
			setLastThreadPoolSize(coreSize);
			threadPool.setThreadSize(coreSize);
			long stopTime = System.currentTimeMillis();
			long elapsedExecutionTime = stopTime - startTime;
			log.debug(String.format("[Tune]: tune duration: %s ms", elapsedExecutionTime));
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

				int corePoolSize = threadPool.getCorePoolSize();
				System.err.println(String.format("Current Thread number: %s", corePoolSize));

				int newThreadSize = corePoolSize - 1;

				if (newThreadSize != 0) {
					threadPool.setThreadSize(newThreadSize);
				}

				if (newThreadSize == 0) {
					System.err.println(
							String.format("We have finished executing %s tasks, we should tune the threadpool...",
									lastThreadPoolSize));
					System.err.println(String.format("Active threads = %s, Queue size = %s",
							threadPool.getActiveCount(), threadPool.getQueue().size()));
					int tuneThreadNum = tune(taskRunner, getLastThreadPoolSize());
					// We stop the sampling timer in the timer itself if tuning is finished.
					if (!isTuningFinished())
						startMonitoring(tuneThreadNum);
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

		threadPool.setThreadSize(minThreadNum);
		setLastThreadPoolSize(minThreadNum);
		super.reset();
	}

}
