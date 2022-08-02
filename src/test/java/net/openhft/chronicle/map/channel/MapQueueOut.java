package net.openhft.chronicle.map.channel;

public interface MapQueueOut<REPLY, QUEUE> {
    REPLY reply();

    QUEUE queue();
}
