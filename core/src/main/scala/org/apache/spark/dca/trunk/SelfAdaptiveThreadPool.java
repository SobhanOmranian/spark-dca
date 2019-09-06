package org.apache.spark.dca.trunk;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.spark.util.UninterruptibleThread;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class SelfAdaptiveThreadPool {
	private ThreadPoolExecutor threadPool;

	public ThreadPoolExecutor getThreadPool() {
		return threadPool;
	}

	public SelfAdaptiveThreadPool() {
		
		ThreadFactory threadFactory = new ThreadFactoryBuilder()
			      .setDaemon(true)
			      .setNameFormat("Executor task launch worker-%d")
			      .setThreadFactory(new ThreadFactory() {

					@Override
					public Thread newThread(Runnable r) {
				          // Use UninterruptibleThread to run tasks so that we can allow running codes without being
				          // interrupted by `Thread.interrupt()`. Some issues, such as KAFKA-1894, HADOOP-10622,
				          // will hang forever if some methods are interrupted.
				          return new UninterruptibleThread(r, "unused"); // thread name will be set by ThreadFactoryBuilder
					}}).build();
			    	  		
			    	  		
		threadPool = (ThreadPoolExecutor)Executors.newCachedThreadPool(threadFactory);
	}

}
