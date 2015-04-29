package net.openhft.chronicle.map;

import net.openhft.chronicle.engine.client.ClientWiredStatelessTcpConnectionHub;
import net.openhft.chronicle.engine.client.ParameterizeWireKey;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.WireKey;
import org.jetbrains.annotations.NotNull;

import java.util.*;

import static net.openhft.chronicle.engine.client.ClientWiredStatelessTcpConnectionHub.CoreFields.reply;
import static net.openhft.chronicle.map.ClientWiredStatelessChronicleEntrySet.EntrySetEventId.*;
import static net.openhft.chronicle.map.ClientWiredStatelessChronicleEntrySet.Params.*;


class ClientWiredStatelessChronicleEntrySet<K, V> extends MapStatelessClient<K, V, ClientWiredStatelessChronicleEntrySet.EntrySetEventId>
        implements Set<Map.Entry<K, V>> {

    public ClientWiredStatelessChronicleEntrySet(@NotNull final String channelName,
                                                 @NotNull final ClientWiredStatelessTcpConnectionHub hub,
                                                 final long cid,
                                                 @NotNull final Class<V> vClass,
                                                 @NotNull final Class<K> kClass) {

        super(channelName, hub, "entrySet", cid, kClass, vClass);
    }


    @Override
    public int size() {
        return proxyReturnInt(size);
    }

    @Override
    public boolean isEmpty() {
        return proxyReturnBoolean(isEmpty);
    }

    @Override
    public boolean contains(Object o) {
        return false;
    }

    @NotNull
    @Override
    public Iterator<Map.Entry<K, V>> iterator() {
        final int numberOfSegments = proxyReturnUint16(numberOfSegements);

        // todo itterate the segments
        return segmentIterator(1);

    }


    /**
     * gets the iterator for a given segment
     *
     * @param segment the maps segment number
     * @return and iterator for the {@code segment}
     */
    @NotNull
    Iterator<Map.Entry<K, V>> segmentIterator(int segment) {

        final Map<K, V> e = new HashMap<K, V>();

        proxyReturnWireConsumerInOut(iterator,

                valueOut -> valueOut.uint16(segment),

                wireIn -> {

                    final ValueIn read = wireIn.read(reply);

                    while (read.hasNextSequenceItem()) {

                        read.sequence(s -> s.marshallable(r -> {
                            final K k = r.read(key).object(kClass);
                            final V v = r.read(value).object(vClass);
                            e.put(k, v);
                        }));
                    }

                    return e;
                });


        return e.entrySet().iterator();
    }

    @NotNull
    @Override
    public Object[] toArray() {
        return new Object[0];
    }

    @NotNull
    @Override
    public <T> T[] toArray(T[] a) {
        return null;
    }

    @Override
    public boolean add(Map.Entry<K, V> kvEntry) {
        return false;
    }


    @Override
    public boolean remove(Object o) {
        return proxyReturnBooleanArgs(remove, o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean addAll(Collection<? extends Map.Entry<K, V>> c) {
        return false;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return false;
    }

    @Override
    public void clear() {

    }

    @Override
    protected boolean eventReturnsNull(@NotNull EntrySetEventId methodName) {
        return false;
    }


    enum Params implements WireKey {
        key,
        value,
        segment
    }

    enum EntrySetEventId implements ParameterizeWireKey {
        size,
        isEmpty,
        remove(key),
        numberOfSegements,
        iterator(segment);

        private final WireKey[] params;

        <P extends WireKey> EntrySetEventId(P... params) {
            this.params = params;
        }

        public <P extends WireKey> P[] params() {
            return (P[]) this.params;

        }
    }
}