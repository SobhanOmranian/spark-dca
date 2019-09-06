package org.apache.spark.dca;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.apache.spark.dca.socket.SocketConnection;
import org.apache.spark.dca.strace.IStraceReader;
import org.apache.spark.executor.Executor.TaskRunner;

public abstract class AdaptiveTuner extends Tuner{
	private final static Logger log = Logger.getLogger("AdaptiveTuner");

	public Sampler sampler;

	private AtomicInteger finishedTasksNum = new AtomicInteger(0);
	private AtomicInteger submittedTasksNum = new AtomicInteger(0);
	private AtomicInteger totalSubmittedTasksNum = new AtomicInteger(0);
	protected AtomicInteger lastThreadPoolSize = new AtomicInteger(0);

	private ConcurrentHashMap<Long, TaskRunner> runningTasks = new ConcurrentHashMap<Long, TaskRunner>();
	private ConcurrentHashMap<Long, List<Long>> taskStartStatus = new ConcurrentHashMap<Long, List<Long>>();

	protected ConcurrentSkipListMap<Integer, ArrayList<Float>> monitoringResults = new ConcurrentSkipListMap<Integer, ArrayList<Float>>();

	private AtomicInteger currentLineNumberIoStat = new AtomicInteger(0);

	protected Queue<Long> completedTasksThroughputs = new ConcurrentLinkedQueue<Long>();

	private AtomicBoolean monitoringStarted = new AtomicBoolean(false);
	private AtomicLong monitorStartTime = new AtomicLong(0);
	private AtomicLong globalTime = new AtomicLong(0);

	private AtomicLong totalCompletedTasksReadData = new AtomicLong(0);
	private AtomicLong totalSamplingReadDataForInterval = new AtomicLong(0);
	private AtomicLong totalSamplingWrittenDataForInterval = new AtomicLong(0);
	private AtomicLong totalSamplingGcTimeForInterval = new AtomicLong(0);
	private AtomicLong totalSamplingExecutionTimeForInterval = new AtomicLong(0);

	public AdaptiveTuner() {
	}

	public void initialise() {
		log.info("Initializing...");
		
		
		setMonitorStartTime(System.currentTimeMillis());
		setGlobalTime(System.currentTimeMillis());
		this.sampler.startTimer();
	}

	public Integer getSubmittedTasksNum() {
		return submittedTasksNum.get();
	}

	public void setSubmittedTasksNum(Integer x) {
		this.submittedTasksNum.set(x);
	}

	public void incSubmitedTasksNum() {
		this.submittedTasksNum.getAndIncrement();
	}

	public void incTotalSubmitedTasksNum() {
		this.totalSubmittedTasksNum.getAndIncrement();
	}

	public int getFinishedTasksNum() {
		return finishedTasksNum.get();
	}

	public void setFinishedTasksNum(Integer x) {
		this.finishedTasksNum.set(x);
	}

	public void incFinishedTasksNum() {
		this.finishedTasksNum.getAndIncrement();
	}

	

	public void afterExecute(Runnable r, Throwable t) {

		if (r instanceof TaskRunner) {
			TaskRunner taskRunner = (TaskRunner) r;
			Long finishTime = System.currentTimeMillis() - getGlobalTime();
			List<Long> status = taskStartStatus.get(taskRunner.taskId());
			if (status != null) {
				Long startCores = status.get(0);
				Long startTime = status.get(1);
				taskFinishOutputWriter.write(String.format("%s,%s,%s,%s,%s,%s", executorId, taskRunner.taskId(),
						startTime, finishTime, startCores + 0.5, threadPool.getCorePoolSize()));
			}

			log.info(String.format("Current running threads: %s, coreSize: %s, maxSize: %s, queueSize: %s",
					threadPool.getActiveCount(), threadPool.getCorePoolSize(), threadPool.getMaximumPoolSize(),
					threadPool.getQueue().size()));
			if (!isTuningFinished()) {

				// Remove the current task from the list of running tasks.
				runningTasks.remove(taskRunner.taskId());

				// Add excess read data
				// sampler.addFinishedDataToTotalSamplingReadDataForInterval(taskRunner);
				sampler.addFinishedDataToTotalSamplingReadDataForInterval(taskRunner);

				log.debug(String.format("Finished executing task %s (i.e., afterExecute())", taskRunner.taskId()));

				// This is a special case where the first task is finished much earlier than the
				// others.
				// We ignore it for now.
				if (finishedTasksNum.get() == 0 && taskRunner.taskId() == 0) {
					log.debug(String.format("Special case where the first task is finished much earlier than others",
							taskRunner.taskId()));
					return;
				}

				// Increment number of finished tasks
				incFinishedTasksNum();

				// Calculate the io throughput of the completed task
				Long bytesRead = taskRunner.getBytesRead();
				Long executorRunTime = taskRunner.getExecutorRunTime();
				// Float executorRunTimeFloat = executorRunTime.floatValue();
				Long completedTaskThroughput = 0L;
				if (executorRunTime != 0) {
					completedTaskThroughput = bytesRead / (executorRunTime); // byte per millisecond
					completedTaskThroughput = (completedTaskThroughput / 1024); // kb per millisecond
					// completedTaskThroughput = (completedTaskThroughput / 1024); // mb per
					// millisecond
					completedTasksThroughputs.add(completedTaskThroughput);
				} else {
					log.debug(String.format("[WARNING]: Task %t has %s execution time.", taskRunner.taskId(),
							executorRunTime));
				}
				totalCompletedTasksReadData.getAndAdd(bytesRead);
				log.debug(String.format("bytesRead = %s B, executorRunTime = %s ms, throughput = %s MBps", bytesRead,
						executorRunTime, completedTaskThroughput));
			}

		}

	};

	public void startMonitoring(int threadNum) {
		Long now = System.currentTimeMillis();
		log.debug(String.format("[Monitor]: Starting monitoring for %s threads at time %s...", threadNum, now));
		setMonitoringStarted(true);

		straceReader.startMonitoring(threadPool.getCurrentStage());

		if (getIoStatFilePath() != "") {
			setCurrentLineNumberIoStat(MyUtils.readFileInList(getIoStatFilePath()).size());
			log.debug(String.format("Starting monitoring ioStat at line: %s", getCurrentLineNumberIoStat()));
		}

		// Reset the number of submitted tasks for next iteration.
		setSubmittedTasksNum(0);
		setFinishedTasksNum(0);

		setMonitorStartTime(now);
		setTotalCompletedTasksReadData(0L);
		setTotalSamplingReadDataForInterval(0L);
		setTotalSamplingWrittenDataForInterval(0L);
		setTotalSamplingGcTimeForInterval(0L);
		setTotalSamplingExecutionTimeForInterval(0L);

		completedTasksThroughputs.clear();

		sampler.startTimer();

	}

	public void beforeExecute(Thread t, Runnable r) {
		synchronized (this) {
			if (r instanceof TaskRunner) {
				TaskRunner taskRunner = (TaskRunner) r;
				taskStartStatus.put(taskRunner.taskId(), Arrays.asList(Long.valueOf(threadPool.getCorePoolSize()),
						System.currentTimeMillis() - getGlobalTime()));
				if (!isTuningFinished()) {
					if (getTotalSubmittedTasksNum() == 0 && !isMonitoringStarted()) {
						log.debug(String.format("[MONITOR]: Start monitoring as first task has been submitted!"));
						startMonitoring(threadPool.getCorePoolSize());
					}
					incSubmitedTasksNum();
					incTotalSubmitedTasksNum();
					log.debug(String.format("We are going to execute task %s (i.e., beforeExecute())",
							taskRunner.taskId()));
					runningTasks.put(taskRunner.taskId(), taskRunner);

				}
			}
		}
	}

	public float getAverageDiskThroughput(String logPath) throws IOException {
		float total = 0;

		List<String> lines = MyUtils.readFileInListFromTo(logPath, currentLineNumberIoStat.get());
		if (lines == null) {
			return 0;
		}
		currentLineNumberIoStat.addAndGet(lines.size());
		log.debug(String.format("currentLineNumberIoStat is now: %s", currentLineNumberIoStat));

		if (lines.get(0).startsWith("Date"))
			lines.remove(0);

		for (String line : lines) {
			total += MyUtils.getReadThroughputFromLine(line);
		}

		log.debug(String.format("total throughput: %s, size: %s", total, lines.size()));
		float avg = total / lines.size();

		return avg;
	}


//	@Override
//	public void execute(Runnable command) {
//		super.execute(command);
//	}

	public void reset() {
		setTuningFinished(false);
		sampler.clearSamplingResults();
		sampler.clearPreviousValues();

		taskStartStatus.clear();
		// This should trigger the startMonitoring method on the next [beforeExecute()]
		// call.
		setTotalSubmittedTasksNum(0);

		runningTasks.clear();

		setMonitoringStarted(false);
	}

	public void reportEverything(int threadNum) {
		float total = 0;

		float avgDiskThroughput = 0;
		Float totalTaskThroughput = 0F; // unit: byte
		Float totalTaskReadThroughputFromSampling = 0F;
		Float totalTaskWriteThroughputFromSampling = 0F;
		Float totalTaskBothThroughputFromSampling = 0F;
		Float totalGcPercentageFromSampling = 0F;
		

		// Report all sampling throughputs
		sampler.reportSamples();

		// Report average disk throughput (iostat)
		try {
			if (getIoStatFilePath() != "") {
				avgDiskThroughput = getAverageDiskThroughput(getIoStatFilePath());
				// avgDiskThroughput = 0;
				log.debug(String.format("[%s THREADS] => disk throughput: %s", threadNum, avgDiskThroughput));
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			log.debug("[Exception] => IO exception while getting iostat average disk throughput: " + e.getMessage());
		}

		Float avgTaskThroughput = (float) MyUtils.calculateAverage(completedTasksThroughputs);
		log.debug(String.format("[%s THREADS] - average throughput of completed tasks: %s", threadNum,
				avgTaskThroughput));

		Long now = System.currentTimeMillis();
		Long totalTime = (now - getMonitorStartTime());
		Float totalTimeFloat = totalTime.floatValue(); // unit: ms
		log.debug(String.format("[%s THREADS] - Monitoring started: %s, now: %s, diff = %s", threadNum,
				getMonitorStartTime(), now, totalTimeFloat));

		if (totalTime != 0) {
			// Report Total task throughput in MB/s
			totalTaskThroughput = getTotalCompletedTasksReadData().floatValue() / (totalTimeFloat / 1000);
			totalTaskThroughput = (totalTaskThroughput / 1024); // KB per second
			totalTaskThroughput = (totalTaskThroughput / 1024); // MB per second
			log.debug(String.format(
					"[%s THREADS]- totalBytesRead = %s B, totalTime = %s ms, totalTaskThroughput = %s MBps", threadNum,
					getTotalCompletedTasksReadData(), totalTimeFloat, totalTaskThroughput));

			// Report read throughput from samples in B/s
			totalTaskReadThroughputFromSampling = getTotalSamplingReadDataForInterval().floatValue()
					/ (totalTimeFloat / 1000);
			totalTaskReadThroughputFromSampling = totalTaskReadThroughputFromSampling / 1024; // KBps
			totalTaskReadThroughputFromSampling = totalTaskReadThroughputFromSampling / 1024; // MBps
			log.debug(String.format(
					"[Sample] [%s THREADS] - allDataRead = %s B, totalTime = %s ms, total task read throughput from sampling: %s",
					threadNum, getTotalSamplingReadDataForInterval(), totalTimeFloat,
					totalTaskReadThroughputFromSampling));

			// Report write throughput from samples in B/s
			totalTaskWriteThroughputFromSampling = getTotalSamplingWrittenDataForInterval().floatValue()
					/ (totalTimeFloat / 1000);
			totalTaskWriteThroughputFromSampling = totalTaskWriteThroughputFromSampling / 1024; // KBps
			totalTaskWriteThroughputFromSampling = totalTaskWriteThroughputFromSampling / 1024; // MBps
			log.debug(String.format(
					"[Sample] [%s THREADS] - allDataWritten = %s B, totalTime = %s ms, total task write throughput from sampling: %s",
					threadNum, getTotalSamplingWrittenDataForInterval(), totalTimeFloat,
					totalTaskWriteThroughputFromSampling));

			totalTaskBothThroughputFromSampling = totalTaskWriteThroughputFromSampling
					+ totalTaskReadThroughputFromSampling;
			log.debug(String.format(
					"[Sample] [%s THREADS] - total task throughput for both read and write from sampling: %s",
					threadNum, totalTaskBothThroughputFromSampling));
			
			// Report GC percentage from samples in B/s
			// Total exeuction time
			Long totalTaskExecutionTime = getTotalSamplingExecutionTimeForInterval();
			totalGcPercentageFromSampling = (getTotalSamplingGcTimeForInterval().floatValue() * 100)
					/ totalTaskExecutionTime;
			totalGcPercentageFromSampling /= getFinishedTasksNum();
			
			log.info(String.format(
					"[Sample] [%s THREADS] - allGcTime = %s ms, totalTaskExecutionTime = %s ms, totalTime = %s ms, finishedTaskNum = %s, total task GC percentage from sampling: %s",
					threadNum, getTotalSamplingGcTimeForInterval(), totalTaskExecutionTime, totalTimeFloat, getFinishedTasksNum(),
					totalGcPercentageFromSampling));
		}

		// Report average task throughput in MB/s
		float avgTaskThroughputFromSampling = sampler.getTaskThroughputAverageForThreadNum(threadNum);
		log.debug(String.format("[Sample] [%s THREADS] - average task throughput from sampling = %s MBps", threadNum,
				avgTaskThroughputFromSampling));

//		try {
			// total = 0;
			float normalisedByNumberOfFinishedTasks = 0f;
			float normalisedByElapsedTime = 0f;
			float normalisedByBoth = 0f;
			float normalisedBySamplingThroughput = 0f;
			// if (getStraceFilePath() != "") {

			// total = getTotalEpollWaitTime(getStraceFilePath());
			try {
				total = straceReader.getAggregatedEpollWaitTime();
			}
			catch(Exception ex) {
				total = 0;
			}
			
			float overheadTime = 0f;
			
			try {
				overheadTime = straceReader.getOverheadTime().floatValue();
			}
			catch(Exception ex) {
				overheadTime = 0;
			}

			log.debug(String.format("[EPOLL] [%s THREADS] - Total epoll_wait for the interval: %s", threadNum, total));

			// normalisedByNumberOfFinishedTasks = total / threadPool.getCorePoolSize();
			normalisedByNumberOfFinishedTasks = total / getFinishedTasksNum();
			log.debug(String.format("[EPOLL] [%s THREADS] - Normalised epoll_wait by numberOfFinishedTasks[%s] = %s",
					threadNum, getFinishedTasksNum(), normalisedByNumberOfFinishedTasks));

			long elapsedTime = TimeUnit.MILLISECONDS.toSeconds((System.currentTimeMillis() - getMonitorStartTime()));
			// float normalisedByElapsedTime = total / elapsedTime;
			// float normalisedByBoth = total / (threadPool.getCorePoolSize() *
			// elapsedTime);
			// normalisedByBoth = (total * elapsedTime) / threadPool.getCorePoolSize();

			// normalisedByElapsedTime = total * elapsedTime;
			// normalisedByBoth = (total * elapsedTime) / getFinishedTasksNum();

			normalisedByElapsedTime = total / elapsedTime;
			normalisedByBoth = total / (getFinishedTasksNum() * elapsedTime);
			log.debug(String.format("[EPOLL] [%s THREADS] - Normalised epoll_wait by elapsedTime = %s ", threadNum,
					normalisedByElapsedTime));
			log.debug(String.format("[EPOLL] [%s THREADS] - Normalised by finishedTasks[%s] and elapsedTime = %s ",
					threadNum, getFinishedTasksNum(), normalisedByBoth));

			normalisedBySamplingThroughput = total / (getFinishedTasksNum() * totalTaskBothThroughputFromSampling);
			log.debug(String.format("[EPOLL] [%s THREADS] - Normalised by samplingThroughput[%s] = %s ", threadNum,
					getFinishedTasksNum(), totalTaskBothThroughputFromSampling, normalisedBySamplingThroughput));
			// }

			// Save all monitoring results in [monitoringResults] for later processing.
			ArrayList<Float> values = new ArrayList<Float>(Arrays.asList(total, normalisedByNumberOfFinishedTasks,
					normalisedByElapsedTime, normalisedByBoth, normalisedBySamplingThroughput, avgDiskThroughput,
					avgTaskThroughput, totalTaskThroughput.floatValue(), totalTaskReadThroughputFromSampling,
					totalTaskWriteThroughputFromSampling, totalTaskBothThroughputFromSampling,
					avgTaskThroughputFromSampling, totalGcPercentageFromSampling, overheadTime, totalTimeFloat));
			monitoringResults.put(threadNum, values);
//		} catch (Exception e) {
//			// TODO: handle exception
//			log.error(String.format("[Error in reading strace: %s]", e));
//		}
	}

	public int tune(TaskRunner tr, int threadNum) {

		log.debug(String.format("Tuning number of threads after completing %s tasks...", getFinishedTasksNum()));
		reportEverything(threadNum);
		return -1;
	}



	public Boolean isMonitoringStarted() {
		return monitoringStarted.get();
	}

	public void setMonitoringStarted(Boolean val) {
		this.monitoringStarted.set(val);
	}

	public int getCurrentLineNumberIoStat() {
		return currentLineNumberIoStat.get();
	}

	public void setCurrentLineNumberIoStat(int val) {
		this.currentLineNumberIoStat.set(val);
	}

	public String getIoStatFilePath() {
		return ioStatFilePath;
	}

	public Long getMonitorStartTime() {
		return monitorStartTime.get();
	}

	public void setMonitorStartTime(Long val) {
		this.monitorStartTime.set(val);
	}

	public Long getTotalSamplingReadDataForInterval() {
		return totalSamplingReadDataForInterval.get();
	}

	public void setTotalSamplingReadDataForInterval(Long val) {
		this.totalSamplingReadDataForInterval.set(val);
	}

	public void getAndAddTotalSamplingReadDataForInterval(Long val) {
		this.totalSamplingReadDataForInterval.getAndAdd(val);
	}
	
	public Long getTotalSamplingGcTimeForInterval() {
		return totalSamplingGcTimeForInterval.get();
	}

	public void setTotalSamplingGcTimeForInterval(Long val) {
		this.totalSamplingGcTimeForInterval.set(val);
	}

	public void getAndAddTotalSamplingGcTimeForInterval(Long val) {
		this.totalSamplingGcTimeForInterval.getAndAdd(val);
	}
	
	public Long getTotalSamplingExecutionTimeForInterval() {
		return totalSamplingExecutionTimeForInterval.get();
	}

	public void setTotalSamplingExecutionTimeForInterval(Long val) {
		this.totalSamplingExecutionTimeForInterval.set(val);
	}

	public void getAndAddTotalSamplingExecutionTimeForInterval(Long val) {
		this.totalSamplingExecutionTimeForInterval.getAndAdd(val);
	}

	public Long getTotalCompletedTasksReadData() {
		return totalCompletedTasksReadData.get();
	}

	public void setTotalCompletedTasksReadData(Long val) {
		this.totalCompletedTasksReadData.set(val);
	}

	public ConcurrentHashMap<Long, TaskRunner> getRunningTasks() {
		return runningTasks;
	}

	@Override
	public int report(int stageId) {
		int optimal = getThreadPool().getInitialMaximumPoolSize();
		float minValue = 99999999;
		float maxValue = 0;
		System.err.println(String.format("=============== REPORT for stage %s ======================", stageId));

		System.err.println(dcaOutputWriter.getFileHeader());
		for (Entry<Integer, ArrayList<Float>> entry : monitoringResults.entrySet()) {
			int numberOfThreads = entry.getKey();
			ArrayList<Float> values = entry.getValue();
			float totallEpollWait = values.get(0);
			float normalisedByThreadNum = values.get(1);
			float normalisedByTime = values.get(2);
			float normalisedByBoth = values.get(3);
			float normalisedByTotalTaskThroughputFromSampling = values.get(4);
			float diskThroughput = values.get(5);
			float avgTaskThroughput = values.get(6);
			float totalTaskThroughput = values.get(7);
			float totalTaskReadThroughputFromSampling = values.get(8);
			float totalTaskWriteThroughputFromSampling = values.get(9);
			float totalTaskBothThroughputFromSampling = values.get(10);
			float avgTaskThroughputFromSampling = values.get(11);
			float totalTaskGcPercentageFromSampling  = values.get(12);
			float totalFileReadTime = values.get(13);
			float totalTime = values.get(14);

			float metric = 0f;
			
			metric = normalisedByTotalTaskThroughputFromSampling;

			 if (metric < minValue) {
				 minValue = metric;
				 optimal = numberOfThreads;
			 }
			
//			if (metric > maxValue) {
//				maxValue = metric;
//				optimal = numberOfThreads;
//			}

			String result = String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s", executorId, stageId,
					numberOfThreads, totallEpollWait, normalisedByThreadNum, normalisedByTime, normalisedByBoth,
					normalisedByTotalTaskThroughputFromSampling, diskThroughput, avgTaskThroughput, totalTaskThroughput,
					totalTaskReadThroughputFromSampling, totalTaskWriteThroughputFromSampling,
					totalTaskBothThroughputFromSampling, avgTaskThroughputFromSampling, totalTaskGcPercentageFromSampling ,totalTime, totalFileReadTime,
					optimal);
			System.err.println(result);
			dcaOutputWriter.write(result);

		}
		System.err.println(String.format("Optimal number of cores = %s", optimal));
		System.err.println("=============================================");

		monitoringResults.clear();
		return threadPool.executor.originalCores();
	}

	public void shutdown() {
		super.shutdown();
		taskFinishOutputWriter.close();
		straceReader.close();
		
	}

	public int getTotalSubmittedTasksNum() {
		return totalSubmittedTasksNum.get();
	}

	public void setTotalSubmittedTasksNum(int val) {
		this.totalSubmittedTasksNum.set(val);
	}

	public int getLastThreadPoolSize() {
		return lastThreadPoolSize.get();
	}

	public void setLastThreadPoolSize(int val) {
		this.lastThreadPoolSize.set(val);
	}

	public Long getGlobalTime() {
		return globalTime.get();
	}

	public void setGlobalTime(Long globalTime) {
		this.globalTime.set(globalTime);
	}



	public IStraceReader getStraceReader() {
		return straceReader;
	}

	public void setStraceReader(IStraceReader straceReader) {
		this.straceReader = straceReader;
	}

	public SelfAdaptiveThreadPoolExecutor getThreadPool() {
		return (SelfAdaptiveThreadPoolExecutor)threadPool;
	}

	public void setThreadPool(SelfAdaptiveThreadPoolExecutor threadPool) {
		this.threadPool = threadPool;
	}

	public Sampler getSampler() {
		return sampler;
	}

	public void setSampler(Sampler sampler) {
		this.sampler = sampler;
	}

	public Long getTotalSamplingWrittenDataForInterval() {
		return totalSamplingWrittenDataForInterval.get();
	}

	public void setTotalSamplingWrittenDataForInterval(Long val) {
		this.totalSamplingWrittenDataForInterval.set(val);
	}

	public void getAndAddTotalSamplingWrittenDataForInterval(Long val) {
		this.totalSamplingWrittenDataForInterval.getAndAdd(val);
	}

}
