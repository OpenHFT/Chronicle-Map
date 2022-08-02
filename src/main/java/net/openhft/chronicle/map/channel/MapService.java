package net.openhft.chronicle.map.channel;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.wire.Marshallable;

public interface MapService<VALUE, REPLY> extends Marshallable {
    void map(ChronicleMap<Bytes<?>, VALUE> map);

    void reply(REPLY REPLY);

    Class<VALUE> valueClass();

    Class<REPLY> replyClass();
}
