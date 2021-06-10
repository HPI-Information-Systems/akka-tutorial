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
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LargeMessage<T> implements Serializable {
        private static final long serialVersionUID = 2940665245810221108L;
        private T message;
        private ActorRef receiver;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BytesMessage implements Serializable {
        private static final long serialVersionUID = 4057807743872319842L;
        private byte[] bytes;
        private int number;
        private ActorRef sender;
        private ActorRef receiver;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LargeMessageHeaderSYNMessage implements Serializable {
        private static final long serialVersionUID = 1083889189975707009L;
        private int count;
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LargeMessageRequestWork implements Serializable {
        private static final long serialVersionUID = 7046663623518753510L;
        private int a = 0;
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

    byte[][] sendParts;
    int sentParts;
    ActorRef sender;
    ActorRef receiver;

    private void handle(LargeMessage<?> largeMessage) {
        Object message = largeMessage.getMessage();
        sender = this.sender();
        receiver = largeMessage.getReceiver();
        ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));

        byte[] serialized = KryoPoolSingleton.get().toBytesWithClass(message);
        sendParts = chunkArray(serialized, MESSAGE_BUFFER_SIZE);
        sentParts = 0;

        receiverProxy.tell(new LargeMessageHeaderSYNMessage(sendParts.length), this.self());
    }


    private void handle(LargeMessageRequestWork message) {
        if(sentParts < sendParts.length) {
            this.sender().tell(new BytesMessage(sendParts[sentParts], sentParts, sender, receiver), this.self());
            sentParts++;
        }
    }

    ////


    //// RECEIVER PROXY

    int receiveCount;
    byte[][] receiveParts;
    int received = 0;

    private void handle(BytesMessage message) {
        receiveParts[message.number] = message.bytes;
        received++;

        if (received == receiveCount) {
            Object deserialized = KryoPoolSingleton.get().fromBytes(flattenArray(receiveParts));
            message.getReceiver().tell(deserialized, message.getSender());
        } else {
            this.getSender().tell(new LargeMessageRequestWork(), this.self());
        }
    }

    private void handle(LargeMessageHeaderSYNMessage message) {
        receiveCount = message.count;
        receiveParts = new byte[receiveCount][];

        this.sender().tell(new LargeMessageRequestWork(), this.self());
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
