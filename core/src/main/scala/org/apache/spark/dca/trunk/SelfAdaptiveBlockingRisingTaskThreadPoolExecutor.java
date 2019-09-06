package org.apache.spark.dca.trunk;
//package org.apache.spark.dca;
//
//import java.io.BufferedReader;
//import java.io.FileInputStream;
//import java.io.FileNotFoundException;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.io.RandomAccessFile;
//import java.lang.management.ManagementFactory;
//import java.lang.management.ThreadInfo;
//import java.lang.management.ThreadMXBean;
//import java.nio.charset.StandardCharsets;
//import java.nio.file.Files;
//import java.nio.file.Paths;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Collections;
//import java.util.List;
//import java.util.Map.Entry;
//import java.util.Vector;
//import java.util.concurrent.BlockingQueue;
//import java.util.concurrent.ConcurrentSkipListMap;
//import java.util.concurrent.ThreadFactory;
//import java.util.concurrent.ThreadPoolExecutor;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicBoolean;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.concurrent.atomic.AtomicLong;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;
//
//import org.apache.spark.util.Utils;
//import org.apache.commons.lang3.tuple.Pair;
//import org.apache.log4j.Logger;
//import org.apache.spark.executor.Executor.TaskRunner;
//
//public class SelfAdaptiveBlockingRisingTaskThreadPoolExecutor extends ThreadPoolExecutor {
//
//	// private int submittedTasksNum = 0;
//	// private int initialMaximumPoolSize = 0;
//	// private int lastThreadPoolSize = 0;
//	// private boolean shouldTune = false;
//	// private boolean isMinReached = false;
//	// private RandomAccessFile raf;
//	// private long fileOffset = 0;
//	// private static int currentLineNumber = 0;
//	// private static int currentStage = 0;
//
//	private final static Logger log = Logger.getLogger("SelfAdaptiveBlockingRisingTaskThreadPoolExecutor");
//	private AtomicInteger submittedTasksNum = new AtomicInteger(0);
//	private AtomicInteger initialMaximumPoolSize = new AtomicInteger(0);
//	private AtomicBoolean isMaxReached = new AtomicBoolean(false);
//	private AtomicInteger currentLineNumber = new AtomicInteger(0);
//	private AtomicInteger currentLineNumberIoStat = new AtomicInteger(0);
//	private AtomicInteger finishedTasksNum = new AtomicInteger(0);
//	private AtomicInteger lastThreadPoolSize = new AtomicInteger(0);
//	private AtomicLong straceStartTime = new AtomicLong(0);
//	private AtomicBoolean isTuning = new AtomicBoolean(false);
//	private AtomicBoolean tuningFinished = new AtomicBoolean(false);
//	private ConcurrentSkipListMap<Integer, ArrayList<Float>> epolls = new ConcurrentSkipListMap<Integer, ArrayList<Float>>();
//	private ArrayList<Long> throughputs = new ArrayList<Long>();
//	private AtomicInteger currentStage = new AtomicInteger(0);
//	private String straceFilePath = "";
//	private String ioStatFilePath = "";
//
//	public SelfAdaptiveBlockingRisingTaskThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime,
//			TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
//		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
//		// TODO Auto-generated constructor stub
//		straceStartTime.set(System.currentTimeMillis());
//		
//		setThreadSize(2);
//		lastThreadPoolSize.set(2);
//		initialMaximumPoolSize.set(2);
//	}
//
//	public List<String> readFileInListFromTo(String fileName, int from) {
//
//		List<String> lines = readFileInList(fileName);
//		int to = lines.size();
//		log.debug(String.format("Reading from lines (%s) to (%s)", from, to));
//		if (to < from) {
//
//			log.debug(String.format("For some reason, to is less than from:  (%s) to (%s)", from, to));
//			return null;
//		}
//
//		return lines.subList(from, to);
//	}
//
//	public static List<String> readFileInList(String fileName) {
//
//		List<String> lines = Collections.emptyList();
//		try {
//			lines = Files.readAllLines(Paths.get(fileName), StandardCharsets.UTF_8);
//		}
//
//		catch (IOException e) {
//
//			// do something
//			e.printStackTrace();
//		}
//		return lines;
//	}
//
//	@Override
//	protected void beforeExecute(Thread t, Runnable r) {
//		if (r instanceof TaskRunner && !tuningFinished.get()) {
//			TaskRunner taskRunner = (TaskRunner) r;
//			submittedTasksNum.getAndIncrement();
//			log.debug(String.format("We are going to execute task %s (i.e., beforeExecute())", taskRunner.taskId()));
//		}
//		super.beforeExecute(t, r);
//	}
//
//	@Override
//	protected void afterExecute(Runnable r, Throwable t) {
//		log.debug(String.format("Current running threads: %s, in queue: %s, coreSize: %s, maxSize: %s", getActiveCount(),
//				getQueue().size(), getCorePoolSize(), getMaximumPoolSize()));
//		if (r instanceof TaskRunner && !tuningFinished.get()) {
//			// submittedTasksNum++;
//			// int mod = submittedTasksNum % getCorePoolSize();
//			// if (mod == 0) {
//			// System.err.println(
//			// String.format("We have executed [%s(%s)] number of tasks, so we should tune
//			// the number of threads!",
//			// getCorePoolSize(), submittedTasksNum));
//			// shouldTune = true;
//			// }
//			TaskRunner taskRunner = (TaskRunner) r;
//			System.err.println(String.format("After Execution of task: %s", taskRunner.taskId()));
//			System.err.println(String.format("Current Thread number: %s", getCorePoolSize()));
//			
//			Long bytesRead = taskRunner.getMetrics().inputMetrics().bytesRead();
//			// ms
//			Long executorRunTime = taskRunner.getMetrics().executorRunTime();
//			Long throughput = bytesRead / (executorRunTime/1000); // byte per second
//			throughput = (throughput / 1024); // kb per second
//			throughput = (throughput / 1024); // mb per second
//			log.debug(String.format("bytesRead = %s, executorRunTime = %s, throuput = %s",
//					bytesRead, executorRunTime, throughput));
//
//			// initialMaximumPoolSize--;
//
//			// int newThreadSize = Math.max(0, getCorePoolSize() - 1);
//			synchronized (this) {
//				throughputs.add(throughput);
//				int newThreadSize = getCorePoolSize() - 1;
//
//				if (newThreadSize != 0) {
//					System.err.println(String.format("Setting core and maximum pool size from %s to %s",
//							getCorePoolSize(), newThreadSize));
//					setCorePoolSize(newThreadSize);
//					setMaximumPoolSize(newThreadSize);
//				}
//
//				if (newThreadSize == 0) {
//					System.err.println(
//							String.format("We have finished executing %s tasks, we should tune the threadpool...",
//									lastThreadPoolSize));
//					try {
//						System.err.println(String.format("Active threads = %s, Queue size = %s", getActiveCount(),
//								getQueue().size()));
//						tune(r);
//					} catch (Exception e) {
//						System.err.println("ERROR: Something went wrong in tuning");
//						e.printStackTrace();
//					}
//				}
//			}
//		}
//		// TODO Auto-generated method stub
//		super.afterExecute(r, t);
//	}
//
//	public float getAverageDiskThroughput(String logPath) throws IOException {
//		float total = 0;
//
//		List<String> lines = readFileInListFromTo(logPath, currentLineNumberIoStat.get());
//		if (lines == null) {
//			return 0;
//		}
//		currentLineNumberIoStat.addAndGet(lines.size());
//		log.debug(String.format("currentLineNumberIoStat is now: %s", currentLineNumberIoStat));
//
//		if (lines.get(0).startsWith("Date"))
//			lines.remove(0);
//
//		for (String line : lines) {
//			total += getReadThroughputFromLine(line);
//		}
//
//		log.debug(String.format("total throughput: %s, size: %s", total, lines.size()));
//		float avg = total / lines.size();
//
//		return avg;
//	}
//
//	public float getReadThroughputFromLine(String line) {
//		float result = 0;
//
//		result = Float.valueOf(line.split(",")[13]);
//
//		return result;
//	}
//
//	public float getTotalEpollWaitTime(String logPath) throws IOException {
//		float total = 0;
//
//		// RandomAccessFile raf = null;
//		//
//		// if (raf == null) {
//		// raf = new RandomAccessFile(logPath, "r");
//		// }
//		// log.debug("Seeking file to position: " + fileOffset);
//		// raf.seek(fileOffset);
//
//		List<String> lines = readFileInListFromTo(logPath, currentLineNumber.get());
//		if (lines == null) {
//			return 0;
//		}
//		currentLineNumber.addAndGet(lines.size());
//		log.debug(String.format("Current line number is now: %s", currentLineNumber));
//
//		for (String line : lines) {
//			total += getTimeFromLine(line);
//		}
//		// String line;
//		// while (line != null) {
//		// log.debug("line = " + line);
//		// fileOffset += line.getBytes().length + 1;
//		// total += getTimeFromLine(line);
//		// line = raf.readLine();
//		// }
//
//		return total;
//	}
//
//	public float getTimeFromLine(String line) {
//		float result = 0;
//		String pattern = "<[\\d.+]*>";
//		Pattern p = Pattern.compile(pattern);
//		Matcher m = p.matcher(line);
//		if (m.find()) {
//			String value = m.group(0);
//			value = value.substring(1, value.length() - 1);
//			result = Float.parseFloat(value);
//		}
//
//		return result;
//	}
//
//	private static int calculateAverage(List<Long> marks) {
//	    long sum = 0;
//	    for (Long mark : marks) {
//	        sum += mark;
//	    }
//	    log.debug(String.format("Sum: %s, size: %s", sum, marks.size()));
//	    return marks.isEmpty()? 0: (int)sum/marks.size();
//	}
//	
//	public void tune(Runnable command) {
//		long startTime = System.currentTimeMillis();
//		TaskRunner taskRunner = (TaskRunner) command;
//		String straceFilePath = taskRunner.getStracePath();
//
//		String ioStatFilePath = taskRunner.getIoStatPath();
//		float avgDiskThroughput = 0;
//		// straceFilePath = "/home/omranian/log.strace";
//
//		log.debug("Tuning number of threads...");
//		int currentPoolSize = lastThreadPoolSize.get();
//
//		try {
//			// log.debug("Reading iostat file at location: " + ioStatFilePath);
//			// List<String> lines = readFileInList(ioStatFilePath);
//			// for (String line : lines) {
//			// log.debug("Line: " + line);
//			// }
//
//			avgDiskThroughput = getAverageDiskThroughput(ioStatFilePath);
//			log.debug(String.format("%s THREADS - disk throughput: %s", currentPoolSize, avgDiskThroughput));
//		}
//
//		catch (Exception e) {
//			// TODO: handle exception
//			e.printStackTrace();
//			log.debug("io exception = " + e.getMessage());
//		}
//		
//		Float avgTaskThroughput = (float)calculateAverage(throughputs);
//		log.debug(String.format("%s THREADS - average task throughput: %s", currentPoolSize, avgTaskThroughput));
//
//		float total = 0;
//		int minThreadNumber = 2;
//		int maxThreadNumber = 32;
//		log.debug("Reading strace file at location: " + straceFilePath);
//		try {
//			
//			total = getTotalEpollWaitTime(straceFilePath);
//			log.debug(
//					String.format("%s THREADS - total epoll_wait time for in threadpool = %s", currentPoolSize, total));
//			log.debug(String.format("%s THREADS - normalised epoll_wait time by number of threads = %s", currentPoolSize,
//					total / currentPoolSize));
//			long elapsedTime = TimeUnit.MILLISECONDS.toSeconds((System.currentTimeMillis() - straceStartTime.get()));
//			log.debug(String.format("%s THREADS - normalised epoll_wait time by elapsed time = %s ", currentPoolSize,
//					total / elapsedTime));
//			log.debug(String.format("%s THREADS - normalised both = %s ", currentPoolSize,
//					total / (currentPoolSize * elapsedTime)));
//
//			ArrayList<Float> values = new ArrayList<Float>(Arrays.asList(total / currentPoolSize, total / elapsedTime,
//					total / (currentPoolSize * elapsedTime), avgDiskThroughput, avgTaskThroughput));
//			epolls.put(currentPoolSize, values);
//
//			if (total != 0) {
//				// Should we go up or go down?
//				int coreSize = initialMaximumPoolSize.get();
//				if (currentPoolSize == minThreadNumber) {
//					// We have reached the bottom. Double the thread number.
//					int newValue = (int) Math.floor(currentPoolSize * 2);
//					coreSize = Math.min(maxThreadNumber, newValue);
//					isMaxReached.set(false);
//
//				} else if (currentPoolSize == maxThreadNumber) {
//					// We have reached the top. Half the thread number.
//					int newValue = (int) Math.ceil(currentPoolSize / 2);
//					coreSize = Math.max(minThreadNumber, newValue);
//					isMaxReached.set(true);
//
//					// We have reached the top; stop.
//					log.debug(String.format("We have reached min number of threads, so reporting..."));
//					int optimal = report(taskRunner.getStageId());
//					setThreadSize(optimal);
//					tuningFinished.set(true);
//					return;
//				} else {
//					// We are between, go up or down.
//					if (!isMaxReached.get()) {
//						// Go up
//						int newValue = (int) Math.floor(currentPoolSize * 2);
//						coreSize = Math.min(maxThreadNumber, newValue);
//					} else {
//						// Go down
//						int newValue = (int) Math.ceil(currentPoolSize / 2);
//						coreSize = Math.max(minThreadNumber, newValue);
//					}
//				}
//
//				// Check whether we really need to change the pool size. If it is already there,
//				
//				setThreadSize(coreSize);
//
//			}
//			long stopTime = System.currentTimeMillis();
//			long elapsedExecutionTime = stopTime - startTime;
//			log.debug(String.format("Tune execution time: %s ms", elapsedExecutionTime));
//		} catch (IOException ex) {
//			// TODO Auto-generated catch block
//			ex.printStackTrace();
//			System.err.println("io exception = " + ex.getMessage());
//		}
//
//	}
//
//	public void setThreadSize(int size) {
//		// Check whether we really need to change the pool size. If it is already there,
//		// we don't need to.
//		log.debug(String.format("Setting number of threads from %s to %s", lastThreadPoolSize.get(), size));
//		lastThreadPoolSize.set(size);
//		if (size == getMaximumPoolSize()) {
//			log.debug(String.format("Number of threads is already at %s, so no need to change", size));
//		} else {
//
//			setCorePoolSize(size);
//			setMaximumPoolSize(size);
//		}
//		
//		int preStartCount = prestartAllCoreThreads();
//		log.debug(String.format("Prestarted %s threads!", preStartCount));
//	}
//
//	private int report(int stageId) {
//		int optimal = initialMaximumPoolSize.get();
//		float minValue = 99999999;
//		float maxValue = 0;
//		System.err.println(String.format("=============== REPORT for stage %s ======================", stageId));
//		System.err.println(String
//				.format("#cores, normalisedByThreadNum, normalisedByTime, normalisedByBoth, diskThroughput, taskThroughput", stageId));
//		for (Entry<Integer, ArrayList<Float>> entry : epolls.entrySet()) {
//			int numberOfThreads = entry.getKey();
//			ArrayList<Float> values = entry.getValue();
//			float normalisedByThreadNum = values.get(0);
//			float normalisedByTime = values.get(1);
//			float normalisedByBoth = values.get(2);
//			float diskThroughput = values.get(3);
//			float avgTaskThroughput = values.get(4);
//			System.err.println(String.format("%s,%s,%s,%s,%s,%s", numberOfThreads, normalisedByThreadNum, normalisedByTime,
//					normalisedByBoth, diskThroughput, avgTaskThroughput));
//
////			if (normalisedByBoth < minValue) {
////				minValue = normalisedByBoth;
////				optimal = numberOfThreads;
////			}
//			
//			if (avgTaskThroughput > maxValue) {
//				maxValue = avgTaskThroughput;
//				optimal = numberOfThreads;
//			}
//		}
//		System.err.println(String.format("Optimal number of cores = %s", optimal));
//		System.err.println("=============================================");
//		
//		return optimal;
//	}
//
//	public void reset() {
//		setThreadSize(2);
//		submittedTasksNum.set(0);
//		throughputs = new ArrayList<Long>();
//		tuningFinished.set(false);
//
//		straceStartTime.set(System.currentTimeMillis());
//		currentLineNumber.set(readFileInList(straceFilePath).size());
//		currentLineNumberIoStat.set(readFileInList(ioStatFilePath).size());
//		log.debug(String.format("Starting monitoring strace at line: %s", currentLineNumber));
//		log.debug(String.format("Starting monitoring ioStat at line: %s", currentLineNumber));
//
//	}
//
//	@Override
//	public void execute(Runnable command) {
//		if (command instanceof TaskRunner) {
//			TaskRunner taskRunner = (TaskRunner) command;
//			straceFilePath = taskRunner.getStracePath();
//			ioStatFilePath = taskRunner.getIoStatPath();
//			log.debug(String.format("Adding task %s to threadpool queue (i.e., execute())", taskRunner.taskId()));
//			// If the stage has changed, reset the threadpool to its initial state.
//			if (currentStage.get() != taskRunner.getStageId()) {
//				System.err.println(
//						String.format("Stage has changed from %s to %s. Resetting the threadpool to inital values...",
//								currentStage, taskRunner.getStageId()));
//				currentStage.set(taskRunner.getStageId());
//				reset();
//			}
//
//		}
//		super.execute(command);
//	}
//
//}
