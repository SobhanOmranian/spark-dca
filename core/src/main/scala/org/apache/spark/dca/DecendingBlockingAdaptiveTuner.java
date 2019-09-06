package org.apache.spark.dca;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.apache.spark.executor.Executor.TaskRunner;

public class DecendingBlockingAdaptiveTuner extends AdaptiveTuner {

	private final static Logger log = Logger.getLogger("DecendingBlockingAdaptiveTuner");

	private AtomicBoolean isMinReached = new AtomicBoolean(false);
	private AtomicBoolean shouldDescend = new AtomicBoolean(false);

	public DecendingBlockingAdaptiveTuner(SelfAdaptiveThreadPoolExecutor threadPool, int maximumPoolSize) {
		setLastThreadPoolSize(maximumPoolSize);
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
			int currentPoolSize = getLastThreadPoolSize();
			// Should we go up or go down?
			// Currently we stop if we reach the bottom;

			if (currentPoolSize == minThreadNumber) {
				// We have reached the bottom. Double the thread number.
				int newValue = (int) Math.floor(currentPoolSize * 2);
				coreSize = Math.min(maxThreadNumber, newValue);
				isMinReached.set(true);
				// We have reached the bottom; stop.
				log.debug(String.format("We have reached min number of threads, so reporting..."));
				int optimal = report(taskRunner.getStageId());
				getThreadPool().setThreadSize(optimal);
				setTuningFinished(true);
				return optimal;
			} else if (currentPoolSize == maxThreadNumber) {
				// We have reached the top. Half the thread number.
				int newValue = (int) Math.ceil(currentPoolSize / 2);
				coreSize = Math.max(minThreadNumber, newValue);
				isMinReached.set(false);
			} else {
				// We are between, go up or down.
				if (isMinReached.get()) {
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
			getThreadPool().setThreadSize(coreSize);

			long stopTime = System.currentTimeMillis();
			long elapsedExecutionTime = stopTime - startTime;
			log.debug(String.format("[Tune]: tune duration: %s ms", elapsedExecutionTime));
			return coreSize;

		} catch (Exception ex) {
			// TODO Auto-generated catch block
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

				int corePoolSize = getThreadPool().getCorePoolSize();
				System.err.println(String.format("Current Thread number: %s, should decend: %s", corePoolSize,
						getShouldDescend()));

				int newThreadSize = corePoolSize - 1;

				if (getLastThreadPoolSize() != 32 && (getFinishedTasksNum() != 0)
						&& (getFinishedTasksNum() % getThreadPool().getMaximumPoolSize() != 0
								|| getFinishedTasksNum() % getThreadPool().getMaximumPoolSize() == 1)) {
					if (!getShouldDescend()) {
						System.err.println(String.format("[Skip!]"));
						return;
					}
				}

				setShouldDescend(true);

				if (newThreadSize != 0) {
					getThreadPool().setThreadSize(newThreadSize);
				}

				if (newThreadSize == 0) {
					System.err.println(
							String.format("We have finished executing %s tasks, we should tune the threadpool...",
									lastThreadPoolSize));
					System.err.println(String.format("Active threads = %s, Queue size = %s",
							getThreadPool().getActiveCount(), getThreadPool().getQueue().size()));
					int tuneThreadNum = tune(taskRunner, getLastThreadPoolSize());
					// We stop the sampling timer in the timer itself if tuning is finished.
					if (!isTuningFinished())
						startMonitoring(tuneThreadNum);
				}

			}
		}
	}

	@Override
	public void startMonitoring(int threadNum) {
		super.startMonitoring(threadNum);
		setShouldDescend(false);
	}

	@Override
	public void beforeExecute(Thread t, Runnable r) {
		super.beforeExecute(t, r);
	}

	@Override
	public void reset() {

		getThreadPool().setThreadSize(getThreadPool().getInitialMaximumPoolSize());
		setShouldDescend(false);
		super.reset();
	}

	public Boolean getShouldDescend() {
		return shouldDescend.get();
	}

	public void setShouldDescend(Boolean val) {
		this.shouldDescend.set(val);
	}

}
