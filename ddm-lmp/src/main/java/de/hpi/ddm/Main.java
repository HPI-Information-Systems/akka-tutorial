package de.hpi.ddm;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

import de.hpi.ddm.configuration.CommandMaster;
import de.hpi.ddm.configuration.CommandSlave;
import de.hpi.ddm.configuration.ConfigurationSingleton;

public class Main {

	public static void main(String[] args) throws Exception {
		
		CommandMaster commandMaster = new CommandMaster();
        CommandSlave commandSlave = new CommandSlave();
        JCommander jCommander = JCommander.newBuilder()
        	.addCommand(MasterSystem.MASTER_ROLE, commandMaster)
            .addCommand(SlaveSystem.SLAVE_ROLE, commandSlave)
            .build();
        
        try {
        	jCommander.parse(args);

            if (jCommander.getParsedCommand() == null)
                throw new ParameterException("No command given.");

            switch (jCommander.getParsedCommand()) {
                case MasterSystem.MASTER_ROLE:
                	ConfigurationSingleton.get().update(commandMaster);
                	
                	MasterSystem.start();
                    break;
                case SlaveSystem.SLAVE_ROLE:
                	ConfigurationSingleton.get().update(commandSlave);
                	
                	SlaveSystem.start();
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
}
