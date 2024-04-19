package net.openhft.chronicle.map.channel;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.map.ChronicleMap;

/**
 * @param <VALUE> the type of the value in the map
 * @param <REPLY> the type of the reply
 */
public abstract class AbstractMapService<VALUE, REPLY> implements MapService<VALUE, REPLY> {
    protected transient ChronicleMap<Bytes<?>, VALUE> map;
    protected transient REPLY reply;
    private transient boolean closed;

    /**
     * Notify that the service should be closed
     */
    public void close() {
        closed = true;
    }

    public boolean isClosed() {
        return closed;
    }

    public void map(ChronicleMap<Bytes<?>, VALUE> map) {
        this.map = map;
    }

    @Override
    public void reply(REPLY reply) {
        this.reply = reply;
    }
}
