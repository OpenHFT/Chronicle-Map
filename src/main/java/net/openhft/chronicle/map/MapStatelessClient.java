package net.openhft.chronicle.map;

import net.openhft.chronicle.engine.client.ClientWiredStatelessTcpConnectionHub;
import net.openhft.chronicle.engine.client.ParameterizeWireKey;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static net.openhft.chronicle.engine.client.ClientWiredStatelessTcpConnectionHub.CoreFields.reply;

/**
 * Created by Rob Austin
 */
public abstract class MapStatelessClient<K, V, E extends ParameterizeWireKey>
        extends AbstactStatelessClient<E> {

    private Class<V> vClass;

    /**
     * @param channelName
     * @param hub
     * @param type        the type of wire handler for example "MAP" or "QUEUE"
     * @param cid         used by proxies such as the entry-set
     */
    public MapStatelessClient(@NotNull String channelName,
                              @NotNull ClientWiredStatelessTcpConnectionHub hub,
                              @NotNull String type,
                              long cid, @NotNull final Class vClass) {
        super(channelName, hub, type, cid);

        this.vClass = vClass;
    }

    @Nullable
    protected <R> R proxyReturnTypedObject(
            @NotNull final E eventId,
            @NotNull final Class<R> resultType,
            @Nullable Object... args) {

        final long startTime = System.currentTimeMillis();
        final long tid = sendEvent(startTime, eventId, toParameters(eventId, args));

        if (eventReturnsNull(eventId))
            return null;

        if (resultType == vClass)
            return (R) readValue(reply, tid, startTime, null);

        else
            throw new UnsupportedOperationException("class of type class=" + resultType +
                    " is not supported");
    }

    @Nullable
    protected <R> R proxyReturnObject(@NotNull final Class<R> rClass,
                                      @NotNull final E eventId, Object field) {

        final long startTime = System.currentTimeMillis();
        final long tid = sendEvent(startTime, eventId, out -> writeField(out, field));

        if (eventReturnsNull(eventId))
            return null;

        if (rClass == vClass)
            return (R) readValue(reply, tid, startTime, null);
        else
            throw new UnsupportedOperationException("class of type class=" + rClass + " is not " +
                    "supported");
    }

    protected abstract boolean eventReturnsNull(@NotNull E methodName);


    protected V readValue(WireKey argName, long tid, long startTime, final V usingValue) {
        assert !hub.outBytesLock().isHeldByCurrentThread();
        long timeoutTime = startTime + hub.timeoutMs;

        hub.inBytesLock().lock();
        try {

            final Wire wireIn = hub.proxyReply(timeoutTime, tid);
            checkIsData(wireIn);


            return readV(argName, wireIn, usingValue);

        } finally {
            hub.inBytesLock().unlock();
        }
    }


    private V readV(WireKey argName, Wire wireIn, V usingValue) {
        return readObject(argName, wireIn, usingValue, vClass);
    }
}
