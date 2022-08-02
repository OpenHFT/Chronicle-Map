package net.openhft.chronicle.map.channel;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.map.ChronicleMap;

public interface MapQueueService<VALUE, REPLY, QUEUE> {
    void map(ChronicleMap<Bytes<?>, VALUE> map);

    void out(MapQueueOut<REPLY, QUEUE> out);

    Class<VALUE> valueClass();

    Class<REPLY> replyClass();

    default Class<QUEUE> queueClass() {
        return (Class) replyClass();
    }
}
