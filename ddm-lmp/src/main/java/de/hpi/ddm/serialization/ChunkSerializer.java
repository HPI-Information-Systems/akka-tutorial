package de.hpi.ddm.serialization;

import akka.serialization.JSerializer;

public class ChunkSerializer  extends JSerializer {

    @Override
    public boolean includeManifest() {
        return false;
    }

    @Override
    public int identifier() {
        return 666;
    }

    @Override
    public byte[] toBinary(Object obj) {
        if (obj instanceof Chunk) {
            return ((Chunk)obj).getBytes();
        }

        throw new IllegalArgumentException(
                String.format("Can't serialize object of type %s in [%s]", obj.getClass(),getClass().getName()));
    }

    @Override
    public Object fromBinaryJava(byte[] bytes, Class<?> clazz) {
        return new Chunk(bytes);
    }
}