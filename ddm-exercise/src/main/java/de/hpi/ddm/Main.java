package de.hpi.ddm;

import de.hpi.ddm.configuration.Configuration;
import de.hpi.ddm.singletons.ConfigurationSingleton;
import de.hpi.ddm.systems.MasterSystem;
import de.hpi.ddm.systems.WorkerSystem;

public class Main {

	public static void main(String[] args) throws Exception {
		Configuration conf = ConfigurationSingleton.get();
		conf.updateWith(args);
		
		if (conf.getRole().equals(MasterSystem.MASTER_ROLE))
	        MasterSystem.start();
	    else
	        WorkerSystem.start();
	}
}
