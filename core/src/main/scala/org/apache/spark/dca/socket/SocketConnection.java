package org.apache.spark.dca.socket;

import org.apache.log4j.Logger;
import org.apache.spark.dca.Client;

public class SocketConnection {
	
	private final static Logger log = Logger.getLogger("SocketConnection");
	
	private String address = "";
	private int port = 0;
	private Client socketConnection;
	
	public SocketConnection(String address, int port) {
		this.address = address;
		this.port = port;
	}
	
	public void connect() {
		log.debug(String.format("[Socket]: Asked to connect to [%s:%s]...", this.address, this.port));
		if (socketConnection == null) {
			try {
				log.debug(String.format("[Socket]: Connecting to [%s:%s]...", address, port));
				socketConnection = new Client(this.address, this.port);
				log.debug(String.format("[Socket]: Successfully connected to [%s:%s]", address, port));
			} catch (Exception e) {
				// TODO Auto-generated catch block

				log.error(String.format("[Socket]: Failed to connect to [%s:%s]", address, port));
				log.error(String.format("[Socket]: Reason: %s", e.toString()));
			}
		} else {
			log.debug(String.format("[Socket]: We are already connected to [%s:%s]...", address, port));
		}
	}

	public void close() {
		if (socketConnection != null) 
		{
			log.debug(String.format("[Socket]: Closing connection to [%s:%s]", address, port));
			socketConnection.close();
		}
	}

	public Client getSocketConnection() {
		return socketConnection;
	}
}
