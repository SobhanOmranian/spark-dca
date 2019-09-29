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

public abstract class Tuner {
	private final static Logger log = Logger.getLogger("Tuner");
	protected OutputWriter dcaOutputWriter;
	protected OutputWriter taskFinishOutputWriter;
	private String appName = "default";
	public String executorId = "";
	protected String ioStatFilePath = "";
	protected IStraceReader straceReader;
	
	private AtomicBoolean tuningFinished = new AtomicBoolean(false);

	public MyThreadPoolExecutor threadPool;

	public Tuner() {

	}

	public int report(int stageId) {
		log.info("REPORT IN TUNER. THIS SHOULD NOT BE CALLED!");
		return -1;
	}
	
	public void execute(Runnable command) {
		if (command instanceof TaskRunner) {
			TaskRunner taskRunner = (TaskRunner) command;

			// Save appNAme
			if (appName == "default") {
				appName = threadPool.executor.sanitizeString(taskRunner.getAppName());
				String appId = taskRunner.getAppId();
				executorId = taskRunner.getExecutorId();
				String finalName = appName + "-" + appId + "#" + executorId;
				
				log.info(
						String.format("Initializing dcaOutputWriter..."));
				if (dcaOutputWriter == null)
					dcaOutputWriter = new DcaResultOutputWriter(finalName);
				taskFinishOutputWriter = new TaskFinishOutputWriter(finalName);

				// Save strace and iostat file paths
				straceReader.setLogPath(threadPool.executor.getStracePath());
				ioStatFilePath = threadPool.executor.getIoStatPath();
			}
			
			String isStaticEnv = System.getenv("SPARK_DCA_STATIC");
			Boolean isStatic = false;

			if (isStaticEnv != null) {
				log.debug("[DCA-CONFIG] [UPDATE-SCHEDULER]: Enabled!");
				isStatic = isStaticEnv.equals("1");
			}

			log.info(String.format("Adding task %s to threadpool queue (i.e., execute())", taskRunner.taskId()));
			if (threadPool.getCurrentStage() != taskRunner.getStageId() && isTuningFinished() == false && !isStatic) {
				log.info(
						String.format("Stage has changed from %s to %s detected in TUNER and tuning is not finished, saving whatever we have...",
								threadPool.getCurrentStage(), taskRunner.getStageId()));
				report(threadPool.getCurrentStage());
			}
			
		}
	}
	
	public void shutdown() {
		if(isTuningFinished() == false && threadPool.getMaximumPoolSize() != Integer.MAX_VALUE)
			report(threadPool.getCurrentStage());
		dcaOutputWriter.close();
	}
	
	public Boolean isTuningFinished() {
		return tuningFinished.get();
	}

	public void setTuningFinished(Boolean tuningFinished) {
		this.tuningFinished.set(tuningFinished);
	};
}
