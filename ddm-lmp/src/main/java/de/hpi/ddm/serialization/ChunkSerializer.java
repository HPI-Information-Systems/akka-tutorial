package de.hpi.ddm.serialization;

import akka.serialization.JSerializer;

public class ChunkSerializer  extends JSerializer {

    // If you need logging here, introduce a constructor that takes an ExtendedActorSystem.
    // public MyOwnSerializer(ExtendedActorSystem actorSystem)
    // Get a logger using:
    // private final LoggingAdapter logger = Logging.getLogger(actorSystem, this);

    // This is whether "fromBinary" requires a "clazz" or not
    @Override
    public boolean includeManifest() {
        return false;
    }

    // Pick a unique identifier for your Serializer,
    // you've got a couple of billions to choose from,
    // 0 - 40 is reserved by Akka itself
    @Override
    public int identifier() {
        return 666;
    }

    // "toBinary" serializes the given object to an Array of Bytes
    @Override
    public byte[] toBinary(Object obj) {
        if (obj.getClass() == Chunk.class) {
            Chunk objCpy = (Chunk) obj;
            return objCpy.getBytes();
        }

        throw new IllegalArgumentException(String.format("Type %s not supported", obj.getClass().getName()));
    }

    // "fromBinary" deserializes the given array,
    // using the type hint (if any, see "includeManifest" above)
    @Override
    public Object fromBinaryJava(byte[] bytes, Class<?> clazz) {
        return new Chunk(bytes);
    }
}