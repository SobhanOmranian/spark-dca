package org.apache.spark.dca;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.CharBuffer;

import org.apache.log4j.Logger;

public class Client {
	public static Long overheadTime = 0L;
	
	private static String getTotalEpollWaitCommand = "GET_EPOLL_WAIT";
	private static String resetTotalEpollWaitCommand = "RESET_EPOLL_WAIT";
	private static String stageChangedCommand = "STAGE_CHANGED";
	
	private final static Logger log = Logger.getLogger("Client");

	// initialize socket and input output streams
	private Socket socket = null;

	PrintWriter outPrint = null;
	OutputStreamWriter outStreamWriter = null;

	BufferedReader br = null;
	BufferedWriter bufOut = null;

	BufferedReader pythonResultBufferdReader = null;

	// constructor to put ip address and port
	public Client(String address, int port) throws UnknownHostException, IOException {
		// establish a connection

		socket = new Socket(address, port);
		System.out.println("Connected");

		outStreamWriter = new OutputStreamWriter(socket.getOutputStream(), "UTF-8");
		bufOut = new BufferedWriter(outStreamWriter);

		pythonResultBufferdReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

	}

	public float getEpollWaitTime() {
		float result = 0;

		sendCommand(getTotalEpollWaitCommand);
		result = Float.parseFloat(readResults());

		return result;
	}

	public void restartEpollWait() {
		sendCommand(resetTotalEpollWaitCommand);
	}
	
	public void notifyStageChanged(int newStageId) {
		sendCommand(stageChangedCommand + "^"  + newStageId);
	}

	private void sendCommand(String cmd) {
		Long startTime = System.currentTimeMillis();
		
		try {
			bufOut.write(cmd);
			bufOut.newLine();
			bufOut.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		Long elapsed = System.currentTimeMillis() - startTime;
		
		log.debug(String.format("[OVERHEAD]: time to send the command[%s] to socket: %s", cmd, elapsed));
		overheadTime += elapsed;
		log.debug(String.format("[OVERHEAD]: total overhead time: %s", overheadTime));
	}

	private String readResults() {
		Long startTime = System.currentTimeMillis();
		String result = "0";
		try {
			char[] bytes = new char[512];
			pythonResultBufferdReader.read(bytes);
			result = String.valueOf(bytes); // for UTF-8 encoding
			log.debug("[Client] [Decoded]: " + result);
		} catch (IOException e) {
			e.printStackTrace();
		}

		Long elapsed = System.currentTimeMillis() - startTime;
		
		log.debug(String.format("[OVERHEAD]: time to read from the socket: %s", elapsed));
		overheadTime += elapsed;
		log.debug(String.format("[OVERHEAD]: total overhead time: %s", overheadTime));
		
		return result;
	}

	public void close() {
		try {
//			out.close();
			socket.close();
			pythonResultBufferdReader.close();
			bufOut.close();
		} catch (IOException i) {
			System.out.println(i);
		}
	}

	public Long getOverheadTime() {
		return overheadTime;
	}
}