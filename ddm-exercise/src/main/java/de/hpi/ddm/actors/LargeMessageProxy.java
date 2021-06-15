package de.hpi.ddm.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import de.hpi.ddm.singletons.KryoPoolSingleton;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

public class LargeMessageProxy extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "largeMessageProxy";

    public static final int MESSAGE_BUFFER_SIZE = 262144;

    public static Props props() {
        return Props.create(LargeMessageProxy.class);
    }

    ////////////////////
    // Actor Messages //
    ////////////////////

    @Data
    public static class LargeMessage<T> implements Serializable {
        private static final long serialVersionUID = 2940665245810221108L;
        private long id;
        private T message;
        private ActorRef receiver;

        public LargeMessage(T message, ActorRef receiver) {
            this.message = message;
            this.receiver = receiver;
            this.id = new Random().nextLong();
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BytesMessage implements Serializable {
        private static final long serialVersionUID = 4057807743872319842L;
        private long id;
        private byte[] bytes;
        private int number;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LargeMessageHeaderSYNMessage implements Serializable {
        private static final long serialVersionUID = 1083889189975707009L;
        private long id;
        private int count;
        private ActorRef sender;
        private ActorRef receiver;
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LargeMessageRequestWork implements Serializable {
        private static final long serialVersionUID = 7046663623518753510L;
        private long id;
    }

    /////////////////
    // Actor State //
    /////////////////

    /////////////////////
    // Actor Lifecycle //
    /////////////////////

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(LargeMessage.class, this::handle)
                .match(BytesMessage.class, this::handle)
                .match(LargeMessageHeaderSYNMessage.class, this::handle)
                .match(LargeMessageRequestWork.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    //// SENDER PROXY

    ConcurrentHashMap<Long, byte[][]> sendParts = new ConcurrentHashMap<>();
    ConcurrentHashMap<Long, Integer> sentParts = new ConcurrentHashMap<>();
    ConcurrentHashMap<Long, ActorRef> sender = new ConcurrentHashMap<>();
    ConcurrentHashMap<Long, ActorRef> receiver = new ConcurrentHashMap<>();

    private void handle(LargeMessage<?> largeMessage) {
        Object message = largeMessage.getMessage();
        ActorRef snd = this.sender();
        ActorRef rcv = largeMessage.getReceiver();
        sender.put(largeMessage.id, snd);
        receiver.put(largeMessage.id, rcv);
        ActorSelection receiverProxy = this.context().actorSelection(rcv.path().child(DEFAULT_NAME));

        byte[] serialized = KryoPoolSingleton.get().toBytesWithClass(message);
        byte[][] parts = chunkArray(serialized, MESSAGE_BUFFER_SIZE);
        sendParts.put(largeMessage.id, parts);
        sentParts.put(largeMessage.id, 0);

        receiverProxy.tell(new LargeMessageHeaderSYNMessage(largeMessage.id, parts.length, snd, rcv), this.self());
    }


    private void handle(LargeMessageRequestWork message) {
        int sent = sentParts.get(message.id);
        this.sender().tell(new BytesMessage(message.id, sendParts.get(message.id)[sent], sent), this.self());
        sentParts.put(message.id, sent + 1);

        if(sent + 1 == sendParts.get(message.id).length){
            sendParts.remove(message.id);
            sentParts.remove(message.id);
            sender.remove(message.id);
            receiver.remove(message.id);
        }
    }

    ////


    //// RECEIVER PROXY

    ConcurrentHashMap<Long, Integer> receiveCount = new ConcurrentHashMap<>();
    ConcurrentHashMap<Long, byte[][]> receiveParts = new ConcurrentHashMap<>();
    ConcurrentHashMap<Long, Integer> received = new ConcurrentHashMap<>();


    private void handle(LargeMessageHeaderSYNMessage message) {
        receiveCount.put(message.id, message.count);
        receiveParts.put(message.id, new byte[message.count][]);
        sender.put(message.id, message.sender);
        receiver.put(message.id, message.receiver);
        received.put(message.id, 0);

        this.sender().tell(new LargeMessageRequestWork(message.id), this.self());
    }

    private void handle(BytesMessage message) {
        int rec = received.get(message.id) + 1;
        receiveParts.get(message.id)[message.number] = message.bytes;
        received.put(message.id, rec);

        if (rec == receiveCount.get(message.id)) {
            Object deserialized = KryoPoolSingleton.get().fromBytes(flattenArray(receiveParts.get(message.id)));
            receiver.get(message.id).tell(deserialized, sender.get(message.id));

            receiveCount.remove(message.id);
            receiveParts.remove(message.id);
            received.remove(message.id);
            sender.remove(message.id);
            receiver.remove(message.id);
        } else {
            this.getSender().tell(new LargeMessageRequestWork(message.id), this.self());
        }
    }

    ////

    private static byte[][] chunkArray(byte[] array, int size) {
        int numOfChunks = (int) Math.ceil((double) array.length / size);
        byte[][] output = new byte[numOfChunks][];

        for (int i = 0; i < numOfChunks; ++i) {
            int start = i * size;
            int length = Math.min(array.length - start, size);

            byte[] temp = new byte[length];
            System.arraycopy(array, start, temp, 0, length);
            output[i] = temp;
        }

        return output;
    }

    // ToDo only one elemnet?
    private static byte[] flattenArray(byte[][] array) {
        byte[] flattenedArray = new byte[(array.length - 1) * array[0].length + array[array.length - 1].length];
        for (int i = 0; i < array.length; i++) {
            System.arraycopy(array[i], 0, flattenedArray, i * array[0].length, array[i].length);
        }
        return flattenedArray;
    }

}
