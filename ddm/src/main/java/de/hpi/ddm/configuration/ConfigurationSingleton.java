package de.hpi.ddm.configuration;

public class ConfigurationSingleton {

	private static Configuration configuration = new Configuration();
	
	public static Configuration get() {
		return configuration;
	}
	
	public static void set(Configuration instance) {
		configuration = instance;
	}
}
