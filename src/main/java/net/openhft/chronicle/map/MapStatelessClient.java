package net.openhft.chronicle.map;

import net.openhft.chronicle.engine.client.ClientWiredStatelessTcpConnectionHub;
import net.openhft.chronicle.engine.client.ParameterizeWireKey;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static net.openhft.chronicle.engine.client.ClientWiredStatelessTcpConnectionHub.CoreFields.reply;

/**
 * Created by Rob Austin
 */
public abstract class MapStatelessClient<K, V, E extends ParameterizeWireKey> extends
        AbstactStatelessClient<E> {

    protected Class<V> vClass;

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

    protected Object readValue(WireKey argName, long tid, long startTime, final V usingValue) {
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

    private Object readV(WireKey argName, Wire wireIn, V usingValue) {
        return readObject(argName, wireIn, usingValue, vClass);
    }

    protected <E> E readObject(WireKey argName, Wire wireIn, E usingValue, Class<E> clazz) {

        final ValueIn valueIn = wireIn.read(argName);
        if (valueIn.isNull())
            return null;

        if (StringBuilder.class.isAssignableFrom(clazz)) {
            valueIn.text((StringBuilder) usingValue);
            return usingValue;
        } else if (Marshallable.class.isAssignableFrom(clazz)) {


            final E v;
            if (usingValue == null)
                try {
                    v = clazz.newInstance();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            else
                v = usingValue;


            valueIn.marshallable((Marshallable) v);
            return v;

        } else if (String.class.isAssignableFrom(clazz)) {
            //noinspection unchecked
            return (E) valueIn.text();

        } else if (Long.class.isAssignableFrom(clazz)) {
            //noinspection unchecked
            return (E) (Long) valueIn.int64();
        } else if (Double.class.isAssignableFrom(clazz)) {
            //noinspection unchecked
            return (E) (Double) valueIn.float64();

        } else if (Integer.class.isAssignableFrom(clazz)) {
            //noinspection unchecked
            return (E) (Integer) valueIn.int32();

        } else if (Float.class.isAssignableFrom(clazz)) {
            //noinspection unchecked
            return (E) (Float) valueIn.float32();

        } else if (Short.class.isAssignableFrom(clazz)) {
            //noinspection unchecked
            return (E) (Short) valueIn.int16();

        } else if (Character.class.isAssignableFrom(clazz)) {
            //noinspection unchecked
            final String text = valueIn.text();
            if (text == null || text.length() == 0)
                return null;
            return (E) (Character) text.charAt(0);

        } else if (Byte.class.isAssignableFrom(clazz)) {
            //noinspection unchecked
            return (E) (Byte) valueIn.int8();


        } else {
            throw new IllegalStateException("unsupported type");
        }
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
    protected <R> R proxyReturnObject(Class<R> rClass, @NotNull final E eventId, Object field) {

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


}
