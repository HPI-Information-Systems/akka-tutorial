package de.hpi.akka_tutorial;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import de.hpi.akka_tutorial.remote.Calculator;

import java.net.InetAddress;
import java.net.UnknownHostException;

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
    private static void startMaster(MasterCommand masterCommand) {
        Calculator.runMaster(masterCommand.host, masterCommand.port, masterCommand.numLocalWorkers);
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
    @Parameters(commandDescription = "start a master")
    static class MasterCommand extends CommandBase {

        public static final int DEFAULT_PORT = 7877;

        @Parameter(names = {"-w", "--workers"}, description = "number of workers to start locally")
        int numLocalWorkers = 0;

        @Override
        int getDefaultPort() {
            return DEFAULT_PORT; // We use twin primes for master and slaves, of course! ;P
        }

    }

    /**
     * Command to start a slave.
     */
    @Parameters(commandDescription = "start a slave")
    static class SlaveCommand extends CommandBase {

        @Override
        int getDefaultPort() {
            return 7879; // We use twin primes for master and slaves, of course! ;P
        }

        @Parameter(names = "--master", description = "host[:port] of the master", required = true)
        String master;

        String getMasterHost() {
            int colonIndex = this.master.lastIndexOf(':');
            if (colonIndex == -1) return this.master;
            return this.master.substring(0, colonIndex);
        }

        int getMasterPort() {
            int colonIndex = this.master.lastIndexOf(':');
            if (colonIndex == -1) return MasterCommand.DEFAULT_PORT;
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
        String host;

        {
            try {
                this.host = InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                this.host = "localhost";
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
