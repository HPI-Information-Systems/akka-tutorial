package de.hpi.akka_tutorial;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import de.hpi.akka_tutorial.remote.Calculator;
import de.hpi.akka_tutorial.remote.actors.scheduling.ReactiveSchedulingStrategy;
import de.hpi.akka_tutorial.remote.actors.scheduling.RoundRobinSchedulingStrategy;
import de.hpi.akka_tutorial.remote.actors.scheduling.SchedulingStrategy;

public class Main {

    public static void main(String[] args) {
        
    	// Parse the command-line args.
        MasterCommand masterCommand = new MasterCommand();
        SlaveCommand slaveCommand = new SlaveCommand();
        JCommander jCommander = JCommander.newBuilder()
                .addCommand("master", masterCommand)
                .addCommand("slave", slaveCommand)
                .build();

        try {
            jCommander.parse(args);

            if (jCommander.getParsedCommand() == null) {
                throw new ParameterException("No command given.");
            }

            // Start a master or slave.
            switch (jCommander.getParsedCommand()) {
                case "master":
                    startMaster(masterCommand);
                    break;
                case "slave":
                    startSlave(slaveCommand);
                    break;
                default:
                    throw new AssertionError();

            }

        } catch (ParameterException e) {
            System.out.printf("Could not parse args: %s\n", e.getMessage());
            if (jCommander.getParsedCommand() == null) {
                jCommander.usage();
            } else {
                jCommander.usage(jCommander.getParsedCommand());
            }
            System.exit(1);
        }

    }

    /**
     * Start a master.
     *
     * @param masterCommand defines the parameters of the master
     */
    private static void startMaster(MasterCommand masterCommand) throws ParameterException {
        SchedulingStrategy.Factory schedulingStrategyFactory;
        switch (masterCommand.schedulingStrategy) {
            case "round-robin":
                schedulingStrategyFactory = new RoundRobinSchedulingStrategy.Factory();
                break;
            case "reactive":
                schedulingStrategyFactory = new ReactiveSchedulingStrategy.Factory();
                break;
            default:
                throw new ParameterException(String.format("Unknown scheduling strategy: %s", masterCommand.schedulingStrategy));
        }
        Calculator.runMaster(masterCommand.host, masterCommand.port, schedulingStrategyFactory, masterCommand.numLocalWorkers);
    }

    /**
     * Start a slave.
     *
     * @param slaveCommand defines the parameters of the slave
     */
    private static void startSlave(SlaveCommand slaveCommand) {
        Calculator.runSlave(slaveCommand.host, slaveCommand.port, slaveCommand.getMasterHost(), slaveCommand.getMasterPort());
    }

    /**
     * Command to start a master.
     */
    @Parameters(commandDescription = "start a master actor system")
    static class MasterCommand extends CommandBase {

        public static final int DEFAULT_PORT = 7877; // We use twin primes for master and slaves, of course! ;P

        @Override
        int getDefaultPort() {
            return DEFAULT_PORT;
        }
        
        /**
         * Defines the number of workers that this actor system should spawn.
         */
        @Parameter(names = {"-w", "--workers"}, description = "number of workers to start locally")
        int numLocalWorkers = 0;

        /**
         * Defines the scheduling strategy to be used in the master.
         */
        @Parameter(names = {"-s", "--scheduler"}, description = "a scheduling strategy (round-robin or reactive)")
        String schedulingStrategy = "reactive";
    }

    /**
     * Command to start a slave.
     */
    @Parameters(commandDescription = "start a slave actor system")
    static class SlaveCommand extends CommandBase {

    	public static final int DEFAULT_PORT = 7879; // We use twin primes for master and slaves, of course! ;P
    	
        @Override
        int getDefaultPort() {
            return DEFAULT_PORT;
        }

        /**
         * Defines the address, i.e., host and port of the master actor system.
         */
        @Parameter(names = {"-m", "--master"}, description = "host[:port] of the master", required = true)
        String master;

        String getMasterHost() {
            int colonIndex = this.master.lastIndexOf(':');
            if (colonIndex == -1) 
            	return this.master;
            return this.master.substring(0, colonIndex);
        }

        int getMasterPort() {
            int colonIndex = this.master.lastIndexOf(':');
            if (colonIndex == -1) {
            	return MasterCommand.DEFAULT_PORT;
            }
            String portSpec = this.master.substring(colonIndex + 1);
            try {
                return Integer.parseInt(portSpec);
            } catch (NumberFormatException e) {
                throw new ParameterException(String.format("Illegal port: \"%s\"", portSpec));
            }
        }

    }

    /**
     * This class defines shared parameters across masters and slaves.
     */
    abstract static class CommandBase {

        /**
         * Defines the address that we want to bind the Akka remoting interface to.
         */
        @Parameter(names = {"-h", "--host"}, description = "host/IP to bind against")
        String host = this.getDefaultHost();

        /**
         * Provide the default host.
         *
         * @return the default host
         */
        String getDefaultHost() {
            try {
                return InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                return "localhost";
            }
        }

        @Parameter(names = {"-p", "--port"}, description = "port to bind against")
        int port = this.getDefaultPort();

        /**
         * Provide the default port.
         *
         * @return the default port
         */
        abstract int getDefaultPort();
    }
}
