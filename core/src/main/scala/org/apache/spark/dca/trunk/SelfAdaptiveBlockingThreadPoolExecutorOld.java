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
//import java.util.Arrays;
//import java.util.Collections;
//import java.util.List;
//import java.util.concurrent.BlockingQueue;
//import java.util.concurrent.ThreadFactory;
//import java.util.concurrent.ThreadPoolExecutor;
//import java.util.concurrent.TimeUnit;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;
//
//import org.apache.spark.util.Utils;
//
//
//import org.apache.spark.executor.Executor.TaskRunner;
//
//public class SelfAdaptiveBlockingThreadPoolExecutorOld extends ThreadPoolExecutor {
//
//	private int submittedTasksNum = 0;
//	private int initialMaximumPoolSize = 0;
//	private int lastThreadPoolSize = 0;
//	private boolean shouldTune = false;
//	private boolean isMinReached = false;
//	private RandomAccessFile raf;
//	private long fileOffset = 0;
//	private static int currentLineNumber = 0;
//	private static int currentStage = 0;
//
//	public SelfAdaptiveBlockingThreadPoolExecutorOld(int corePoolSize, int maximumPoolSize, long keepAliveTime,
//			TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
//		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
//		// TODO Auto-generated constructor stub
//		initialMaximumPoolSize = maximumPoolSize;
//		lastThreadPoolSize = maximumPoolSize;
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
//		int from = currentLineNumber;
//		int to = lines.size();
//		System.err.println(String.format("Reading from lines (%s) to (%s)", from, to));
//		if (to < from) {
//			System.err.println(String.format("For some reason, to is less than from:  (%s) to (%s)", from, to));
//			return null;
//		}
//
//		return lines.subList(from, to);
//	}
//
//	@Override
//	protected void beforeExecute(Thread t, Runnable r) {
//		if (r instanceof TaskRunner) {
//			TaskRunner taskRunner = (TaskRunner) r;
//			System.err.println(String.format("Before executing task: %s", taskRunner.taskId()));
//		}
//		super.beforeExecute(t, r);
//	}
//
//	@Override
//	protected void afterExecute(Runnable r, Throwable t) {
//		// System.err.println("Threadpool: afterExecute!");
//		if (r instanceof TaskRunner) {
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
//			// initialMaximumPoolSize--;
//
////			int newThreadSize = Math.max(0, getCorePoolSize() - 1);
//			int newThreadSize = getCorePoolSize() - 1;
//
//
//
//			if (newThreadSize != 0) {
//				System.err.println(String.format("Setting core and maximum pool size from %s to %s", getCorePoolSize(),
//						newThreadSize));
//				setCorePoolSize(newThreadSize);
//				setMaximumPoolSize(newThreadSize);
//			}
//
//			if (newThreadSize == 0) {
//				System.err.println(
//						String.format("We have finished executing %s tasks, we should tune the threadpool...", lastThreadPoolSize));
//				try {
//					System.err.println(
//							String.format("Active threads = %s, Queue size = %s", getActiveCount(), getQueue().size()));
//					tune(r);
//				} catch (Exception e) {
//					System.err.println("ERROR: Something went wrong in tuning");
//					e.printStackTrace();
//				}
//			}
//
//			// String threadName = taskRunner.threadName();
//			// long threadId = taskRunner.getThreadId();
//			// System.err.println(String.format("Thread [%s] with name [%s] has finished.",
//			// threadId, threadName));
//
//			// ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
//			// ThreadInfo threadInfo = threadMXBean.getThreadInfo(threadId);
//			// System.err.println(String.format("Thread [%s] has spent %ss in WAITING
//			// state.", threadId, threadInfo.getWaitedTime()));
//			// System.err.println(String.format("Thread [%s] current cpu time: %s",
//			// threadId, threadMXBean.getCurrentThreadCpuTime()));
//			//// System.err.println(String.format("Thread [%s] stacktrace:", threadId));
//			// for (StackTraceElement ste : threadInfo.getStackTrace()) {
//			// System.err.println(ste);
//			// }
//
//			// if (taskRunner.shouldOptimise()) {
//			// System.err.println("Task " + taskRunner.taskId() + " says we should
//			// optimise!");
//			// String strace = taskRunner.getStracePath();
//			// List<String> lines = this.readFileInList(strace);
//			//
//			// double epoll = Double.parseDouble(lines.get(0));
//			//
//			// System.err.println("Epoll: " + epoll);
//			// }
//
//		}
//		// TODO Auto-generated method stub
//		super.afterExecute(r, t);
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
//		// System.err.println("Seeking file to position: " + fileOffset);
//		// raf.seek(fileOffset);
//
//		List<String> lines = readFileInList(logPath);
//		if (lines == null) {
//			return 0;
//		}
//		currentLineNumber += lines.size();
//		System.err.println(String.format("Current line number is now: %s", currentLineNumber));
//
//		for (String line : lines) {
//			total += getTimeFromLine(line);
//		}
//		// String line;
//		// while (line != null) {
//		// System.err.println("line = " + line);
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
//	public void tune(Runnable command) {
//		long startTime = System.currentTimeMillis();
//		TaskRunner taskRunner = (TaskRunner) command;
//		String straceFilePath = taskRunner.getStracePath();
//		// straceFilePath = "/home/omranian/log.strace";
//
//		System.err.println("Tuning number of threads...");
//		System.err.println("Reading strace file at location: " + straceFilePath);
//
//		float total = 0;
//		int minThreadNumber = 2;
//		int maxThreadNumber = initialMaximumPoolSize;
//		try {
//			int currentPoolSize = lastThreadPoolSize;
//			total = getTotalEpollWaitTime(straceFilePath);
//			System.err.println(
//					String.format("total epoll_wait time for %s threads in threadpool = %s", currentPoolSize, total));
//			System.err.println("normalised epoll_wait time = " + total / currentPoolSize);
//
//			if (total != 0) {
//				// Should we go up or go down?
//				int coreSize = initialMaximumPoolSize;
//				if (currentPoolSize == minThreadNumber) {
//					// We have reached the bottom. Double the thread number.
//					int newValue = (int) Math.floor(currentPoolSize * 2);
//					coreSize = Math.min(maxThreadNumber, newValue);
//					isMinReached = true;
//				} else if (currentPoolSize== maxThreadNumber) {
//					// We have reached the top. Half the thread number.
//					int newValue = (int) Math.ceil(currentPoolSize / 2);
//					coreSize = Math.max(minThreadNumber, newValue);
//					isMinReached = false;
//				} else {
//					// We are between, go up or down.
//					if (isMinReached) {
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
//				// we don't need to.
//				if (coreSize == currentPoolSize) {
//					System.err.println(
//							String.format("Number of threads is already at %s, so no need to change", coreSize));
//				} else {
//					System.err.println(
//							String.format("Setting number of threads from %s to %s", currentPoolSize, coreSize));
//					lastThreadPoolSize = coreSize;
//					setCorePoolSize(coreSize);
//					setMaximumPoolSize(coreSize);
//				}
//			}
//			long stopTime = System.currentTimeMillis();
//			long elapsedTime = stopTime - startTime;
//			System.err.println(String.format("Tune execution time: %s ms", elapsedTime));
//		} catch (IOException ex) {
//			// TODO Auto-generated catch block
//			ex.printStackTrace();
//			System.err.println("io exception = " + ex.getMessage());
//		}
//
//	}
//	
//	public void reset() {
//		setMaximumPoolSize(initialMaximumPoolSize);
//		setCorePoolSize(initialMaximumPoolSize);
//		submittedTasksNum = 0;
//		lastThreadPoolSize = initialMaximumPoolSize;
//		
//	}
//
//	@Override
//	public void execute(Runnable command) {
//		if (command instanceof TaskRunner) {
//			TaskRunner taskRunner = (TaskRunner) command;
//			System.err.println("Submitting task: " + taskRunner.taskId());
//			// If the stage has changed, reset the threadpool to its initial state.
//			if(currentStage != taskRunner.getStageId()) {
//				System.err.println(String.format("Stage has changed from %s to %s. Resetting the threadpool to inital values...", currentStage, taskRunner.getStageId()));
//				currentStage = taskRunner.getStageId();
//				reset();
//			}
//			
//		}
//		super.execute(command);
//	}
//
//}
