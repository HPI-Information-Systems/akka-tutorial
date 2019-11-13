package de.hpi.ddm.configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;

import lombok.Data;

@Data
public class Configuration {
	
	public static final int DEFAULT_MASTER_PORT = 7877;
	public static final int DEFAULT_SLAVE_PORT = 7879;
	
	private String host = getDefaultHost();			// This machine's host name or IP that we use to bind this application against
	private int port = DEFAULT_MASTER_PORT;			// This machines port that we use to bind this application against
	
	private String masterHost = getDefaultHost();	// The host name or IP of the master; if this is a master, masterHost = host
	private int masterPort = DEFAULT_MASTER_PORT;	// The port of the master; if this is a master, masterPort = port
	
	private String actorSystemName = "ddm";			// The name of this application
	
	private int numWorkers = 4;						// The number of workers to start locally; should be at least one if the algorithm is started standalone (otherwise there are no workers to run the application)
	
	private int dataSize = 20; 						// Size of the data message (in MB) with which each worker should be initialized
	
	private static String getDefaultHost() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            return "localhost";
        }
    }

	public void update(CommandMaster commandMaster) {
		this.host = commandMaster.host;
		this.port = commandMaster.port;
		this.numWorkers = commandMaster.numWorkers;
		this.dataSize = commandMaster.dataSize;
	}

	public void update(CommandSlave commandSlave) {
		this.host = commandSlave.host;
		this.port = commandSlave.port;
		this.masterHost = commandSlave.masterhost;
		this.masterPort = commandSlave.masterport;
		this.numWorkers = commandSlave.numWorkers;
	}
}
