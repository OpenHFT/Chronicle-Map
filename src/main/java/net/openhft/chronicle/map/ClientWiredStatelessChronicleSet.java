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

    private final Function<ValueIn, U> conumer;

    public ClientWiredStatelessChronicleSet(@NotNull final String channelName,
                                            @NotNull final ClientWiredStatelessTcpConnectionHub hub,
                                            final long cid,
                                            @NotNull final Function<ValueIn, U> wireToSet) {

        super(channelName, hub, "entrySet", cid);
        this.conumer = wireToSet;
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
    public Iterator<U> iterator() {
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
    Iterator<U> segmentIterator(int segment) {

        final Set<U> e = new HashSet<U>();

        proxyReturnWireConsumerInOut(iterator,

                valueOut -> valueOut.uint16(segment),

                wireIn -> {

                    final ValueIn read = wireIn.read(reply);

                    while (read.hasNextSequenceItem()) {
                        read.sequence(v -> e.add(conumer.apply(read)));
                    }

                    return e;
                });


        return e.iterator();
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
    public boolean add(U u) {
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
    public boolean addAll(Collection<? extends U> c) {
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
    protected boolean eventReturnsNull(@NotNull SetEventId methodName) {
        return false;
    }


    enum Params implements WireKey {
        key,
        value,
        segment
    }

    enum SetEventId implements ParameterizeWireKey {
        size,
        isEmpty,
        remove(key),
        numberOfSegements,
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