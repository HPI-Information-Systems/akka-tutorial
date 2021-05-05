package de.hpi.ddm.configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

import de.hpi.ddm.singletons.DatasetDescriptorSingleton;
import de.hpi.ddm.structures.BloomFilter;
import de.hpi.ddm.systems.MasterSystem;
import de.hpi.ddm.systems.WorkerSystem;
import lombok.Data;

@Data
public class Configuration {
	
	public static final int DEFAULT_MASTER_PORT = 7877;
	public static final int DEFAULT_WORKER_PORT = 7879;
	
	private String role = MasterSystem.MASTER_ROLE;	// This machine's role in the cluster.
	
	private String host = getDefaultHost();			// This machine's host name or IP that we use to bind this application against
	private int port = DEFAULT_MASTER_PORT;			// This machines port that we use to bind this application against
	
	private String masterHost = getDefaultHost();	// The host name or IP of the master; if this is a master, masterHost = host
	private int masterPort = DEFAULT_MASTER_PORT;	// The port of the master; if this is a master, masterPort = port
	
	private String actorSystemName = "ddm";			// The name of this application
	
	private int numWorkers = 4;						// The number of workers to start locally; should be at least one if the algorithm is started standalone (otherwise there are no workers to run the application)
	
	private boolean startPaused = false;			// Wait for some console input to start; useful, if we want to wait manually until all ActorSystems in the cluster are started (e.g. to avoid work stealing effects in performance evaluations)
	
	private int bufferSize = 50; 					// Buffer for input reading (the DatasetReader pre-fetches and buffers this many records)
	
	private int welcomeDataSize = 0; 				// Size of the welcome message's data (in MB) with which each worker should be greeted
	
	private static String getDefaultHost() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            return "localhost";
        }
    }

	public void updateWith(String[] args) {
		CommandMaster commandMaster = new CommandMaster();
        CommandWorker commandWorker = new CommandWorker();
        JCommander jCommander = JCommander.newBuilder()
        	.addCommand(MasterSystem.MASTER_ROLE, commandMaster)
            .addCommand(WorkerSystem.WORKER_ROLE, commandWorker)
            .build();
        
        try {
        	jCommander.parse(args);

            if (jCommander.getParsedCommand() == null)
                throw new ParameterException("No command given.");

            switch (jCommander.getParsedCommand()) {
                case MasterSystem.MASTER_ROLE:
                	this.role = MasterSystem.MASTER_ROLE;
                	this.update(commandMaster);
                	DatasetDescriptorSingleton.get().update(commandMaster);
                    break;
                case WorkerSystem.WORKER_ROLE:
                	this.role = WorkerSystem.WORKER_ROLE;
                	this.update(commandWorker);
                	DatasetDescriptorSingleton.set(null);
                    break;
                default:
                    throw new AssertionError();
            }
        } catch (ParameterException e) {
            System.out.printf("Could not parse args: %s\n", e.getMessage());
            jCommander.usage();
            System.exit(1);
        }
	}
	
	private void update(CommandMaster commandMaster) {
		this.host = commandMaster.host;
		this.port = commandMaster.port;
		this.numWorkers = commandMaster.numWorkers;
		this.startPaused = commandMaster.startPaused;
		this.bufferSize = commandMaster.bufferSize;
		this.welcomeDataSize = commandMaster.welcomeDataSize;
	}

	private void update(CommandWorker commandWorker) {
		this.host = commandWorker.host;
		this.port = commandWorker.port;
		this.masterHost = commandWorker.masterhost;
		this.masterPort = commandWorker.masterport;
		this.numWorkers = commandWorker.numWorkers;
	}
	
	public BloomFilter generateWelcomeData() {
		return new BloomFilter(8 * 1024 * 1024 * this.welcomeDataSize, true);
	}
}
