package org.apache.spark.dca;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.apache.spark.dca.socket.SocketConnection;
import org.apache.spark.dca.strace.NoStraceReader;
import org.apache.spark.executor.Executor;
import org.apache.spark.executor.Executor.TaskRunner;

public class SelfAdaptiveNoActionPoolExecutor extends MyThreadPoolExecutor {

	private final static Logger log = Logger.getLogger("SelfAdaptiveNoActionPoolExecutor");
	
	public SelfAdaptiveNoActionPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
			BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, Executor executor) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, executor);
	}
}
