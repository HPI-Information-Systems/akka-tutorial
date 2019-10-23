package de.hpi.ddm.configuration;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@Parameters(commandDescription = "start a master actor system")
public class CommandMaster extends Command {

	@Override
	int getDefaultPort() {
		return Configuration.DEFAULT_MASTER_PORT;
	}

	@Parameter(names = { "-ds", "--dataSize" }, description = "Size of the data message (in MB) with which each worker should be initialized.", required = false)
	int dataSize = ConfigurationSingleton.get().getDataSize();
}
