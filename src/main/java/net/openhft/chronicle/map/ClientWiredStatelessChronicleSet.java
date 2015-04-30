package net.openhft.chronicle.map;

import net.openhft.chronicle.engine.client.ClientWiredStatelessTcpConnectionHub;
import net.openhft.chronicle.engine.client.ParameterizeWireKey;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.WireKey;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Function;

import static net.openhft.chronicle.engine.client.ClientWiredStatelessTcpConnectionHub.CoreFields.reply;
import static net.openhft.chronicle.map.ClientWiredStatelessChronicleSet.Params.key;
import static net.openhft.chronicle.map.ClientWiredStatelessChronicleSet.Params.segment;
import static net.openhft.chronicle.map.ClientWiredStatelessChronicleSet.SetEventId.*;


class ClientWiredStatelessChronicleSet<U> extends MapStatelessClient<ClientWiredStatelessChronicleSet.SetEventId>
        implements Set<U> {

    private final Function<ValueIn, U> consumer;

    public ClientWiredStatelessChronicleSet(@NotNull final String channelName,
                                            @NotNull final ClientWiredStatelessTcpConnectionHub hub,
                                            final long cid,
                                            @NotNull final Function<ValueIn, U> wireToSet,
                                            @NotNull final String type) {

        super(channelName, hub, type, cid);
        this.consumer = wireToSet;
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
        return proxyReturnBooleanWithArgs(contains, o);
    }

    @NotNull
    @Override
    public Iterator<U> iterator() {

        // todo improve numberOfSegments
        final int numberOfSegments = proxyReturnUint16(SetEventId.numberOfSegments);

        // todo iterate the segments for the moment we just assume one segment
        return segmentSet(1).iterator();

    }


    /**
     * gets the iterator for a given segment
     *
     * @param segment the maps segment number
     * @return and iterator for the {@code segment}
     */
    @NotNull
    private Set<U> segmentSet(int segment) {

        final Set<U> e = new HashSet<U>();

        proxyReturnWireConsumerInOut(iterator,

                valueOut -> valueOut.uint16(segment),

                wireIn -> {

                    final ValueIn read = wireIn.read(reply);

                    while (read.hasNextSequenceItem()) {
                        read.sequence(v -> e.add(consumer.apply(read)));
                    }

                    return e;
                });


        return e;
    }

    @NotNull
    @Override
    public Object[] toArray() {
        return asSet().toArray();
    }

    @NotNull
    private Set<U> asSet() {

        final Set<U> e = new HashSet<U>();
        final int numberOfSegments = proxyReturnUint16(SetEventId.numberOfSegments);

        for (long j = 0; j < numberOfSegments; j++) {

            final long i = j;
            proxyReturnWireConsumerInOut(iterator,

                    valueOut -> valueOut.uint16(i),

                    wireIn -> {

                        final ValueIn read = wireIn.read(reply);

                        while (read.hasNextSequenceItem()) {
                            read.sequence(v -> e.add(consumer.apply(read)));
                        }

                        return e;
                    });
        }
        return e;
    }

    @NotNull
    @Override
    public <T> T[] toArray(T[] array) {
        return asSet().toArray(array);
    }

    @Override
    public boolean add(U u) {
        return proxyReturnBoolean(add);
    }


    @Override
    public boolean remove(Object o) {
        return proxyReturnBooleanWithArgs(remove, o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return proxyReturnBooleanWithSequence(containsAll, c);
    }

    @Override
    public boolean addAll(Collection<? extends U> c) {
        return proxyReturnBooleanWithSequence(addAll, c);
    }


    @Override
    public boolean retainAll(Collection<?> c) {
        return proxyReturnBooleanWithSequence(retainAll, c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return proxyReturnBooleanWithSequence(removeAll, c);
    }

    @Override
    public void clear() {
        proxyReturnVoid(clear);
    }

    enum Params implements WireKey {
        key,
        value,
        segment
    }

    enum SetEventId implements ParameterizeWireKey {
        size,
        isEmpty,
        add,
        addAll,
        retainAll,
        containsAll,
        removeAll,
        clear,
        remove(key),
        numberOfSegments,
        contains,
        iterator(segment);

        private final WireKey[] params;

        <P extends WireKey> SetEventId(P... params) {
            this.params = params;
        }

        public <P extends WireKey> P[] params() {
            return (P[]) this.params;

        }
    }
}