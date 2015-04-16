package net.openhft.chronicle.map;

import net.openhft.chronicle.engine.client.ClientWiredStatelessTcpConnectionHub;
import net.openhft.chronicle.engine.client.ParameterizeWireKey;
import net.openhft.chronicle.wire.ValueOut;
import net.openhft.chronicle.wire.WireKey;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static net.openhft.chronicle.map.ClientWiredStatelessChronicleEntrySet.EventId.*;
import static net.openhft.chronicle.map.ClientWiredStatelessChronicleEntrySet.Params.key;


class ClientWiredStatelessChronicleEntrySet<K, V> extends MapStatelessClient<K, V, ClientWiredStatelessChronicleEntrySet.EventId>
        implements Set<Map.Entry<K, V>> {

    public ClientWiredStatelessChronicleEntrySet(@NotNull final String channelName,
                                                 @NotNull final ClientWiredStatelessTcpConnectionHub hub,
                                                 final long cid,
                                                 @NotNull final Class<V> vClass) {

        super(channelName, hub, "entrySet", cid, vClass);
    }


    @Override
    protected Consumer<ValueOut> toParameters(@NotNull EventId eventId, Object... args) {

        return out -> {
            final WireKey[] paramNames = eventId.params();

            if (paramNames.length == 1) {
                writeField(out, args[0]);
                return;
            }

            assert args.length == paramNames.length :
                    "looks like you are missing the paramameter in the EventId, EventId=" +
                            eventId +
                            ", args.length=" + args.length +
                            ", paramNames.length=" + paramNames.length;

            out.marshallable(m -> {

                for (int i = 0; i < paramNames.length; i++) {
                    final ValueOut vo = m.write(paramNames[i]);
                    this.writeField(vo, args[i]);
                }

            });

        };
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
        return null;
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
        return proxyReturnBoolean(remove, o);
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
    protected boolean eventReturnsNull(@NotNull EventId methodName) {
        return false;
    }


    enum Params implements WireKey {
        key
    }

    enum EventId implements ParameterizeWireKey {
        size,
        isEmpty,
        remove(key);

        private final WireKey[] params;

        <P extends WireKey> EventId(P... params) {
            this.params = params;
        }

        public <P extends WireKey> P[] params() {
            return (P[]) this.params;

        }
    }
}