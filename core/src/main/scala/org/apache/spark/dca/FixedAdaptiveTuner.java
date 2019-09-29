package org.apache.spark.dca;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.apache.spark.executor.Executor.TaskRunner;

public class FixedAdaptiveTuner extends Tuner {

	private final static Logger log = Logger.getLogger("FixedAdaptiveTuner");

	public FixedAdaptiveTuner(MyThreadPoolExecutor _threadPool) {
		threadPool = _threadPool;
		threadPool.setCurrentStage(-1);
	}
	
	public void initialise() {
	}

	@Override
	public int report(int stageId) {
		String execId = threadPool.executor.getExecutorId();
		String result = String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s", execId, stageId,
				 threadPool.getMaximumPoolSize(), 0, 0, 0, 0,
				0, 0, 0, 0,
				0, 0,
				0, 0, 0, 0, 0,
				threadPool.getMaximumPoolSize());
		System.err.println(result);
		dcaOutputWriter.write(result);
		return threadPool.getMaximumPoolSize();
	}
	
	public int reportNonIo(int stageId, String finalAppName) {
		String execId = threadPool.executor.getExecutorId();
		String result = String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s", execId, stageId,
				 threadPool.getMaximumPoolSize(), 0, 0, 0, 0,
				0, 0, 0, 0,
				0, 0,
				0, 0, 0, 0, 0,
				threadPool.executor.originalCores());
		System.err.println(result);
		if (dcaOutputWriter == null) {
			// This might happen because in static mode, we save the dca result before any tasks are executed, which is when Tuner sets up dcaOutputWriter.
			// We initialise it here in such a case.

			dcaOutputWriter = new DcaResultOutputWriter(finalAppName);
		}
		dcaOutputWriter.write(result);
		return threadPool.executor.originalCores();
	}
	
	public int reportIo(int stageId, String finalAppName) {
		String execId = threadPool.executor.getExecutorId();
		String result = String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s", execId, stageId,
				 threadPool.getMaximumPoolSize(), 0, 0, 0, 0,
				0, 0, 0, 0,
				0, 0,
				0, 0, 0, 0, 0,
				threadPool.getMaximumPoolSize());
		System.err.println(result);
		if (dcaOutputWriter == null) {
			// This might happen because in static mode, we save the dca result before any tasks are executed, which is when Tuner sets up dcaOutputWriter.
			// We initialise it here in such a case.

			dcaOutputWriter = new DcaResultOutputWriter(finalAppName);
		}
		dcaOutputWriter.write(result);
		return threadPool.getMaximumPoolSize();
	}

	@Override
	public void execute(Runnable command) {
		super.execute(command);
		if (command instanceof TaskRunner) {
			TaskRunner taskRunner = (TaskRunner) command;

//			// If the stage has changed, reset the threadpool to its initial state.
//			if (threadPool.getCurrentStage() != taskRunner.getStageId()) {
//				log.info(
//						String.format("Stage has changed from %s to %s detected in FixedAdaptiveTuner. Saving the DCA for stage %s",
//								threadPool.getCurrentStage(), taskRunner.getStageId(), threadPool.getCurrentStage()));
//				report(taskRunner.getStageId());
//			}
		}
	}

//	@Override
//	public Boolean isTuningFinished() {
//		// TODO Auto-generated method stub
//		return true;
//	}
}
