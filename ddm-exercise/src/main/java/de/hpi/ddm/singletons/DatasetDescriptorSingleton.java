package de.hpi.ddm.singletons;

import de.hpi.ddm.configuration.DatasetDescriptor;

public class DatasetDescriptorSingleton {

	private static DatasetDescriptor datasetDescriptor = new DatasetDescriptor();
	
	public static DatasetDescriptor get() {
		return datasetDescriptor;
	}
	
	public static void set(DatasetDescriptor instance) {
		datasetDescriptor = instance;
	}
}
