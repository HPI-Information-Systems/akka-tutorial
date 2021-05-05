package de.hpi.ddm.singletons;

import de.hpi.ddm.configuration.Configuration;

public class ConfigurationSingleton {

	private static Configuration configuration = new Configuration();
	
	public static Configuration get() {
		return configuration;
	}
	
	public static void set(Configuration instance) {
		configuration = instance;
	}
}
