package org.apache.spark.dca.strace;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.apache.spark.dca.AdaptiveTuner;
import org.apache.spark.dca.Client;

public class PythonFileStraceReader implements IStraceReader {

	private final static Logger log = Logger.getLogger("PythonFileStraceReader");

	private Client socketConnection;

	private AdaptiveTuner tuner;

	private String socketAddress = "127.0.0.1";
	private int socketPort = 12006;

	public PythonFileStraceReader(AdaptiveTuner tuner) {
		connect();
		this.tuner = tuner;
	}

	private void connect() {
		log.debug(String.format("[Socket]: Asked to connect to [%s:%s]...", socketAddress, socketPort));
		if (socketConnection == null) {
			try {
				log.debug(String.format("[Socket]: Connecting to [%s:%s]...", socketAddress, socketPort));
				socketConnection = new Client(socketAddress, socketPort);
				log.debug(String.format("[Socket]: Successfully connected to [%s:%s]", socketAddress, socketPort));
			} catch (Exception e) {
				// TODO Auto-generated catch block

				log.error(String.format("[Socket]: Failed to connect to [%s:%s]", socketAddress, socketPort));
				log.error(String.format("[Socket]: Reason: %s", e.toString()));
			}
		} else {
			log.debug(String.format("[Socket]: We are already connected to [%s:%s]...", socketAddress, socketPort));
		}
	}

	@Override
	public float getAggregatedEpollWaitTime() {


		log.debug(String.format("[Socket]: Contacting python for epoll wait time"));
		float epollWait = socketConnection.getEpollWaitTime();
		log.debug(String.format("[Socket]: Received epoll value: %s", epollWait));

		return epollWait;
	}

	@Override
	public void startMonitoring(int stageId) {
		log.debug(String.format("[Socket][Start-Monitoring]: Asked to send stage changed message to the python socket..."));
		if (socketConnection == null) {
			log.debug(String.format("[Socket][Start-Monitoring]: Python socket server is not yet up, retry connecting in 1 sec..."));
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			connect();
		} else {
			log.debug(String.format("[Socket][Start-Monitoring]: Sending stage changed message to the python socket..."));
			socketConnection.notifyStageChanged(stageId);
		}
	}

	@Override
	public void setLogPath(String path) {
		log.error("[Socket]: There is no log file for Python Strace Reader!");
	}

	@Override
	public void close() {
		log.debug(String.format("[Socket]: Closing connection to [%s:%s]", socketAddress, socketPort));
		socketConnection.close();

	}

	@Override
	public Long getOverheadTime() {
		return socketConnection.getOverheadTime();
	}

}
