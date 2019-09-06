package org.apache.spark.dca;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.apache.spark.executor.Executor.TaskRunner;

public class NoAdaptiveTuner extends Tuner {

	private final static Logger log = Logger.getLogger("NoAdaptiveTuner");
	
	public NoAdaptiveTuner() {
	}

	@Override
	public int report(int stageId) {
		log.info(String.format("Saving DCA result for stage %s and the selection is: %s", stageId, threadPool.getMaximumPoolSize()));
		String result = String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s", executorId, stageId,
				 threadPool.getMaximumPoolSize(), 0, 0, 0, 0,
				0, 0, 0, 0,
				0, 0,
				0, 0, 0, 0,
				threadPool.getMaximumPoolSize());
		System.err.println(result);
		
		dcaOutputWriter.write(result);
		return threadPool.getMaximumPoolSize();
	}

}
