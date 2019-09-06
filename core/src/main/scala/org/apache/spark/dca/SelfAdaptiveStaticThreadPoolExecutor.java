package org.apache.spark.dca;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.apache.spark.dca.socket.SocketConnection;
import org.apache.spark.executor.Executor;
import org.apache.spark.executor.Executor.TaskRunner;

public class SelfAdaptiveStaticThreadPoolExecutor extends MyThreadPoolExecutor {

	private final static Logger log = Logger.getLogger("SelfAdaptiveNoActionPoolExecutor");
	String ioStageListString = null;
	String ioStageThreadNum = null;

	public SelfAdaptiveStaticThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime,
			TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, Executor executor) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, executor);

		ioStageListString = System.getenv("SPARK_DCA_IO_STAGE_LIST");
		ioStageThreadNum = System.getenv("SPARK_DCA_IO_STAGE_THREADNUM");
	}

	@Override
	public void execute(Runnable command) {

		if (command instanceof TaskRunner) {
			TaskRunner taskRunner = (TaskRunner) command;

			// If the stage has changed, reset the threadpool to its initial state.
			if (getCurrentStage() != taskRunner.getStageId()) {
				log.info(String.format(
						"[STATIC]: Stage has changed from %s to %s. Checking if it is in the IO Stage List...",
						getCurrentStage(), taskRunner.getStageId()));

				if (ioStageListString != null && ioStageThreadNum != null) {
					List<String> ioStagesList = Arrays.asList(ioStageListString.split(","));
					if (ioStagesList.contains(String.valueOf(taskRunner.getStageId()))) {
						log.info(String.format("[STATIC]: Stage %s is IO", taskRunner.getStageId()));
						log.info(String.format("[STATIC]: Setting the number of threads to %s", ioStageThreadNum));
						setThreadSize(Integer.parseInt(ioStageThreadNum));
					}
					else {
						log.info(String.format("[STATIC]: Stage %s is NOT IO", taskRunner.getStageId()));
						log.info(String.format("[STATIC]: Setting the number of threads to maximum number of threads", getMaximumPoolSize()));
						setThreadSize(getMaximumPoolSize());
					}
				}

			}
		}

		// TODO Auto-generated method stub
		super.execute(command);

	}

}
