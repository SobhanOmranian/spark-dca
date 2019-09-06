package org.apache.spark.dca;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.apache.spark.executor.Executor.TaskRunner;

public class Sampler {
	private final static Logger log = Logger.getLogger("Sampler");

	private Timer sampleTimer = null;
	private SelfAdaptiveThreadPoolExecutor threadpool;
	private float interval = 1000f;

	private AtomicLong lastSampleTime = new AtomicLong(0);

	private ConcurrentHashMap<Long, ArrayList<Long>> previousReadValues = new ConcurrentHashMap<Long, ArrayList<Long>>();
	private HashMap<Long, ArrayList<Long>> previousWrittenValues = new HashMap<Long, ArrayList<Long>>();
	private HashMap<Long, Long> previousGcValues = new HashMap<Long, Long>();
	private ConcurrentHashMap<Integer, ArrayList<Float>> samplingResults = new ConcurrentHashMap<Integer, ArrayList<Float>>();

	public Sampler(SelfAdaptiveThreadPoolExecutor threadpool) {
		this.threadpool = threadpool;
	}

	public void stopTimer() {
		sampleTimer.cancel();
		sampleTimer = null;
	}

	public void startTimer() {
		if (sampleTimer == null) {
			sampleTimer = new Timer();

			TimerTask sampleTask = new TimerTask() {
				public void run() {
					if (threadpool.getTuner().isTuningFinished()) {
						log.debug(String.format("Cancling sampling timer as tuning is finished."));
						stopTimer();
						return;
					}

					log.debug(String.format("Timer invoked!"));
					sample(interval, null);
				}
			};

			sampleTimer.schedule(sampleTask, 1000, (int) interval);
		}
	}

	public void sample(float interval, TaskRunner finishedTask) {
		log.debug(String.format("Sampling at time: %s ms, interval: %s ms",
				System.currentTimeMillis() - getLastSampleTime(), interval));
		lastSampleTime.set(System.currentTimeMillis());
		AdaptiveTuner tuner = threadpool.getTuner();
		ConcurrentHashMap<Long, TaskRunner> runningTasks = tuner.getRunningTasks();

		if (runningTasks.isEmpty()) {
			log.debug(String.format("No running tasks detected, skip sampling..."));
			return;
		}

		Float allTaskThroughPut = 0F;
		Float allTaskGcPercentage = 0F;
		Float everyDataRead = 0F;
		Float everyDataWritten = 0F;
		Float everyGcTime = 0F;

		int length = runningTasks.size();
		int activeThreadsNum = threadpool.getActiveCount();
		for (Map.Entry<Long, TaskRunner> entry : runningTasks.entrySet()) {
			Long key = entry.getKey();
			TaskRunner taskRunner = entry.getValue();

			Long previousBytesRead = 0L;
			Long previousExecutorRunTime = 0L;
			if (previousReadValues.containsKey(key)) {
				// Task has been running...
				// Calculate deltas from the previous values
				ArrayList<Long> values = previousReadValues.get(key);
				previousBytesRead = values.get(0);
				previousExecutorRunTime = values.get(1);
			}

			Long bytesRead = taskRunner.getBytesReadAll() - previousBytesRead;
			Float bytesReadFloat = bytesRead.floatValue();

			// ms
			Long executorRunTime = taskRunner.getCurrentExecutionTime() - previousExecutorRunTime;
			Float executorRunTimeFloat = executorRunTime.floatValue();

			previousReadValues.put(key, new ArrayList<Long>(
					Arrays.asList(bytesRead + previousBytesRead, executorRunTime + previousExecutorRunTime)));

			// For writes
			Long previousBytesWritten = 0L;
			if (previousWrittenValues.containsKey(key)) {
				// Task has been running...
				// Calculate deltas from the previous values
				ArrayList<Long> values = previousWrittenValues.get(key);
				previousBytesWritten = values.get(0);
			}

			Long bytesWritten = taskRunner.getBytesWrittenAll() - previousBytesWritten;
			if (bytesWritten < 0) {
				log.info(String.format(
						"[Sampler] [CRITICAL]: NEGATIVE VALUE: bytesWritten => %s, current => %s, previous =? %s",
						bytesWritten, taskRunner.getBytesWrittenAll(), previousBytesWritten));
			}
			Float bytesWrittenFloat = bytesWritten.floatValue();

			if (bytesWritten != 0)
				log.debug(String.format("[Sampler] [CRITICAL]: bytesWritten => %s", bytesWritten));

			previousWrittenValues.put(key, new ArrayList<Long>(
					Arrays.asList(bytesWritten + previousBytesWritten, executorRunTime + previousExecutorRunTime)));

			// For GC
			Long previousGcTime = 0L;
			if (previousGcValues.containsKey(key)) {
				// Task has been running...
				// Calculate deltas from the previous values
				previousGcTime = previousGcValues.get(key);
			}
			Long gcTime = taskRunner.getGcTime() - previousGcTime;
			Float gcTimeFloat = gcTime.floatValue();
			previousGcValues.put(key, gcTime + previousGcTime);
			Float gcPercentage = (gcTimeFloat * 100) / executorRunTimeFloat;
			log.debug(String.format("Task %s GC percentage: (%s * 100) / %s = %s percent", key, gcTimeFloat,
					executorRunTimeFloat, gcPercentage));

			Float throughput = bytesReadFloat / (executorRunTimeFloat / 1000); // byte per second
			throughput = (throughput / 1024); // kb per second
			throughput = (throughput / 1024); // mb per second

			allTaskThroughPut += throughput;
			everyDataRead += bytesReadFloat;
			everyDataWritten += bytesWrittenFloat;

			allTaskGcPercentage += gcPercentage;
			everyGcTime += gcTimeFloat;

			tuner.getAndAddTotalSamplingReadDataForInterval(bytesRead);
			tuner.getAndAddTotalSamplingWrittenDataForInterval(bytesWritten);
			tuner.getAndAddTotalSamplingGcTimeForInterval(gcTime);
			tuner.getAndAddTotalSamplingExecutionTimeForInterval(executorRunTime);

		}

		Float avgTaskThroughputFloat = allTaskThroughPut / length;
		log.debug(String.format("[Sampler]: Avg Throughput for %s threads = %s MBps", activeThreadsNum,
				avgTaskThroughputFloat));

		Float avgTaskGcTimeFloat = allTaskGcPercentage / length;
		log.info(String.format("[Sampler]: Avg Task GC Time for %s threads = %s MBps", activeThreadsNum,
				avgTaskGcTimeFloat));

		Float overallGcTime = everyGcTime / (interval); // ms/ms
		log.debug(
				String.format("[Sampler]: overall GC Time  for %s threads = %s MBps", activeThreadsNum, overallGcTime));

		Float overallReadThroughput = everyDataRead / (interval / 1000); // BpS
		overallReadThroughput = overallReadThroughput / 1024; // KBpS
		overallReadThroughput = overallReadThroughput / 1024; // MBpS

		log.debug(String.format("[Sampler]: overall read Throughput for %s threads = %s MBps", activeThreadsNum,
				overallReadThroughput));

		// Save the calculated throughput for [activeThreadsNum] number of threads
		ArrayList<Float> throughputs = samplingResults.get(activeThreadsNum);
		if (throughputs == null) {
			throughputs = new ArrayList<Float>();
			samplingResults.put(activeThreadsNum, throughputs);
		}

		throughputs.add(overallReadThroughput);
	}

	public void reportSamples() {
		log.debug(String.format("Reporting samples:"));
		for (Map.Entry<Integer, ArrayList<Float>> entry : samplingResults.entrySet()) {
			Integer threadNum = entry.getKey();
			String csv = "";
			csv = String.format("%s%s", csv, threadNum);
			ArrayList<Float> throughputs = entry.getValue();
			for (Float t : throughputs) {
				csv = String.format("%s,%s", csv, t);
			}
			log.debug(csv);
		}
	}

	public float getTaskThroughputAverageForThreadNum(int threadNum) {
		log.info(String.format("asked for average taskthroughput for thread num: %s", threadNum));
		ArrayList<Float> throughputs = samplingResults.get(threadNum);
		if (throughputs.isEmpty())
			return 0;
		long sum = 0;
		int skips = 0;
		for (float throughput : throughputs) {
			if (throughput <= 50) {
				skips++;
			} else
				sum += throughput;
		}

		log.debug(String.format("We have %s samples and %s skips:", throughputs.size(), skips));
		if (throughputs.size() - skips == 0)
			return 0;

		return sum / (throughputs.size() - skips);
	}

	public void clearPreviousValues() {
		previousReadValues.clear();
	}

	public void clearSamplingResults() {
		samplingResults.clear();
	}

	public Long getLastSampleTime() {
		return lastSampleTime.get();
	}

	public void addFinishedDataToTotalSamplingReadDataForInterval(TaskRunner taskRunner) {
		Long key = taskRunner.taskId();
		Long previousExecutorRunTime = 0L;

		// For reads
		Long previousBytesRead = 0L;
		if (previousReadValues.containsKey(key)) {
			// Task has been running...
			// Calculate deltas from the previous values
			ArrayList<Long> values = previousReadValues.get(key);
			previousBytesRead = values.get(0);
			previousExecutorRunTime = values.get(1);
		}

		Long bytesRead = taskRunner.getBytesReadAll() - previousBytesRead;
		Float bytesReadFloat = bytesRead.floatValue();

		// ms
		Long executorRunTime = taskRunner.getCurrentExecutionTime() - previousExecutorRunTime;
		Float executorRunTimeFloat = executorRunTime.floatValue();

		previousReadValues.put(key, new ArrayList<Long>(
				Arrays.asList(bytesRead + previousBytesRead, executorRunTime + previousExecutorRunTime)));

		// For writes
		Long previousBytesWritten = 0L;
		if (previousWrittenValues.containsKey(key)) {
			// Task has been running...
			// Calculate deltas from the previous values
			ArrayList<Long> values = previousWrittenValues.get(key);
			previousBytesWritten = values.get(0);
		}

		Long bytesWritten = taskRunner.getBytesWrittenAll() - previousBytesWritten;
		if (bytesWritten < 0) {
			log.info(String.format(
					"[Sampler] [FINISHED] [CRITICAL]: NEGATIVE VALUE: bytesWritten => %s, current => %s, previous =? %s",
					bytesWritten, taskRunner.getBytesWrittenAll(), previousBytesWritten));
		}
		Float bytesWrittenFloat = bytesWritten.floatValue();

		if (bytesWritten != 0)
			log.debug(String.format("[Sampler] [CRITICAL] [FINISHED]: bytesWritten => %s", bytesWritten));

		previousWrittenValues.put(key, new ArrayList<Long>(
				Arrays.asList(bytesWritten + previousBytesWritten, executorRunTime + previousExecutorRunTime)));

		// For GC
		Long previousGcTime = 0L;
		if (previousGcValues.containsKey(key)) {
			// Task has been running...
			// Calculate deltas from the previous values
			previousGcTime = previousGcValues.get(key);
		}
		Long gcTime = taskRunner.getGcTime() - previousGcTime;
		Float gcTimeFloat = gcTime.floatValue();
		previousGcValues.put(key, gcTime + previousGcTime);
		Float gcPercentage = (gcTimeFloat * 100) / executorRunTimeFloat;
		log.info(String.format("[Sampler] [FINISHED]: Task %s GC percentage: (%s * 100) / %s = %s percent", key, gcTimeFloat,
				executorRunTimeFloat, gcPercentage));

		Float throughput = bytesReadFloat / (executorRunTimeFloat / 1000); // byte per second
		throughput = (throughput / 1024); // kb per second
		throughput = (throughput / 1024); // mb per second

		threadpool.getTuner().getAndAddTotalSamplingReadDataForInterval(bytesRead);
		threadpool.getTuner().getAndAddTotalSamplingWrittenDataForInterval(bytesWritten);
		threadpool.getTuner().getAndAddTotalSamplingGcTimeForInterval(gcTime);
		threadpool.getTuner().getAndAddTotalSamplingExecutionTimeForInterval(executorRunTime);
	}
}
