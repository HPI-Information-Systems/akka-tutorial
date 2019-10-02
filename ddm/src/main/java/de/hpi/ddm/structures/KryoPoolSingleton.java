package de.hpi.ddm.structures;

import com.twitter.chill.KryoInstantiator;
import com.twitter.chill.KryoPool;

public class KryoPoolSingleton {

	private static final int POOL_SIZE = 10;
	private static final KryoPool kryo = KryoPool.withByteArrayOutputStream(POOL_SIZE, new KryoInstantiator());
	
	public static KryoPool get() {
		return kryo;
	}
}
