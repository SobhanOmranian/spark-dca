package org.apache.spark.dca;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.apache.spark.dca.socket.SocketConnection;
import org.apache.spark.executor.Executor;
import org.apache.spark.executor.Executor.TaskRunner;

public abstract class MyThreadPoolExecutor extends ThreadPoolExecutor {

	private final static Logger log = Logger.getLogger("MyThreadPoolExecutor");

	public Executor executor;
	private SocketConnection topSocket = new SocketConnection("127.0.0.1", 12002);
	// private SocketConnection memstallSocket = new SocketConnection("127.0.0.1",
	// 12003);
	private SocketConnection mpstatSocket = new SocketConnection("127.0.0.1", 12004);
	private SocketConnection iostatSocket = new SocketConnection("127.0.0.1", 12005);
	private AtomicInteger currentStage = new AtomicInteger(0);
	private Boolean isUpdateSchedulerEnabled = false;

	public MyThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
			BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, Executor executor) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);

		this.executor = executor;

		// int pid = executor.getPid();
		// executor.attachStraceToHdfs();

		String updateSchedulerEnv = System.getenv("SPARK_DCA_UPDATE_SCHEDULER");

		if (updateSchedulerEnv != null) {
			log.debug("[DCA-CONFIG] [UPDATE-SCHEDULER]: Enabled!");
			isUpdateSchedulerEnabled = updateSchedulerEnv.equals("1");
		}

		String pioOutputFileName = "";
		String appName = executor.getAppName();
		String hostName = MyUtils.getHostName();
		String executorId = executor.getExecutorId();
		String appId = executor.getAppId();

		pioOutputFileName = String.format("%s-%s-exec%s-%s", appName, hostName, executorId, appId);

		executor.startPio(pioOutputFileName);

		executor.startMpstat();

		executor.startTop();

		// executor.attachVtune();
		// executor.startMemStall();

		executor.startIoStatMonitoring();

		topSocket.connect();
		// memstallSocket.connect();
		mpstatSocket.connect();
		iostatSocket.connect();

	}

	@Override
	public void execute(Runnable command) {
		super.execute(command);

		if (topSocket.getSocketConnection() == null) {
			log.info(String.format("[Socket][Top] Still could not connect to top socket, retrying..."));
			topSocket.connect();
		}

		// if (memstallSocket.getSocketConnection() == null) {
		// log.debug(String.format("[Socket][MemStall] Still could not connect to
		// memstall socket, retrying..."));
		// memstallSocket.connect();
		// }

		if (mpstatSocket.getSocketConnection() == null) {
			log.debug(String.format("[Socket][Mpstat] Still could not connect to mpstat socket, retrying..."));
			mpstatSocket.connect();
		}

		if (iostatSocket.getSocketConnection() == null) {
			log.debug(String.format("[Socket][Iostat(M)] Still could not connect to iostat socket, retrying..."));
			iostatSocket.connect();
		}

		if (command instanceof TaskRunner) {
			TaskRunner taskRunner = (TaskRunner) command;

			// If the stage has changed, reset the threadpool to its initial state.
			if (getCurrentStage() != taskRunner.getStageId()) {
				notifySockets(taskRunner.getStageId());
			}
		}
	}
	
	public void notifySockets(int stageId) {
		log.debug(String.format("Stage has changed from %s to %s. Resetting the threadpool to inital values...",
				getCurrentStage(), stageId));
		setCurrentStage(stageId);

		log.info(String.format("[Socket][All] Letting all the listening sockets know the stage has changed"));
		if (topSocket.getSocketConnection() != null)
			topSocket.getSocketConnection().notifyStageChanged(stageId);
		// memstallSocket.getSocketConnection().notifyStageChanged(taskRunner.getStageId());
		if (mpstatSocket.getSocketConnection() != null)
			mpstatSocket.getSocketConnection().notifyStageChanged(stageId);
		if (iostatSocket.getSocketConnection() != null)
			iostatSocket.getSocketConnection().notifyStageChanged(stageId);
	}

	public int getCurrentStage() {
		return currentStage.get();
	}

	public void setCurrentStage(int val) {
		this.currentStage.set(val);
	}

	@Override
	public void shutdown() {
		topSocket.close();
		// memstallSocket.close();
		mpstatSocket.close();
		iostatSocket.close();
		super.shutdown();
	}

	public void setThreadSize(int size) {
		// Check whether we really need to change the pool size. If it is already there,
		// we don't need to.
		log.info(String.format("Setting number of threads from %s to %s", getMaximumPoolSize(), size));
		if (size == getMaximumPoolSize()) {
			log.info(String.format("Number of threads is already at %s, so no need to change", size));
		} else {
			log.info(String.format("[MSG]: sending new threadpool core [%s] from Threadpool to Executor...", size));
			if (isUpdateSchedulerEnabled)
				executor.sendThreadPoolUpdateMsg(size);
			setCorePoolSize(size);
			setMaximumPoolSize(size);
		}

		int preStartCount = prestartAllCoreThreads();
		log.info(String.format("Prestarted %s threads!", preStartCount));
	}

}
