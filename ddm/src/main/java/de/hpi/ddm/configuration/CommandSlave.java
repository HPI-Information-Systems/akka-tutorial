package de.hpi.ddm.configuration;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@Parameters(commandDescription = "start a slave actor system")
public class CommandSlave extends Command {

	@Override
	int getDefaultPort() {
		return Configuration.DEFAULT_SLAVE_PORT;
	}

	@Parameter(names = { "-mh", "--masterhost" }, description = "The host name or IP of the master", required = true)
	String masterhost;

	@Parameter(names = { "-mp", "--masterport" }, description = "The port of the master", required = false)
	int masterport = Configuration.DEFAULT_MASTER_PORT;

}
