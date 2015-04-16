/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map;

import net.openhft.chronicle.engine.client.ClientWiredStatelessTcpConnectionHub;
import net.openhft.chronicle.engine.client.ClientWiredStatelessTcpConnectionHub.CoreFields;
import net.openhft.chronicle.hash.function.SerializableFunction;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static java.util.Collections.emptyList;
import static net.openhft.chronicle.engine.client.ClientWiredStatelessTcpConnectionHub.CoreFields.reply;
import static net.openhft.chronicle.map.MapWireHandler.EventId;
import static net.openhft.chronicle.map.MapWireHandler.EventId.*;


/**
 * @author Rob Austin.
 */
class ClientWiredStatelessChronicleMap<K, V> extends MapStatelessClient<K, V, EventId>
        implements ChronicleMap<K, V>, Cloneable, ChannelFactory {

    private static final Logger LOG =
            LoggerFactory.getLogger(ClientWiredStatelessChronicleMap.class);


    public static final Consumer<ValueOut> VOID_PARAMETERS = out -> out.marshallable(AbstactStatelessClient.EMPTY);

    protected Class<K> kClass;

    private boolean putReturnsNull;
    private boolean removeReturnsNull;

    // used with toString()
    private static final int MAX_NUM_ENTRIES = 20;

    public ClientWiredStatelessChronicleMap(
            @NotNull final ClientWiredChronicleMapStatelessBuilder config,
            @NotNull final Class kClass,
            @NotNull final Class vClass,
            @NotNull final String channelName,
            ClientWiredStatelessTcpConnectionHub hub) {
        super(channelName, hub, "MAP", 0, vClass);

        this.putReturnsNull = config.putReturnsNull();
        this.removeReturnsNull = config.removeReturnsNull();
        this.kClass = kClass;
    }


    @SuppressWarnings("UnusedDeclaration")
    void identifier(int localIdentifier) {
        hub.localIdentifier = localIdentifier;
    }

    @Override
    public void getAll(File toFile) throws IOException {
        JsonSerializer.getAll(toFile, this, emptyList());
    }

    @Override
    public void putAll(File fromFile) throws IOException {
        JsonSerializer.putAll(fromFile, this, emptyList());
    }

    @Override
    public V newValueInstance() {
        return (V) VanillaChronicleMap.newInstance(vClass, false);
    }

    @Override
    public K newKeyInstance() {
        return VanillaChronicleMap.newInstance(kClass, true);
    }

    @Override
    public Class<K> keyClass() {
        return kClass;
    }

    @Override
    public boolean forEachEntryWhile(Predicate<? super MapKeyContext<K, V>> predicate) {
        // TODO implement!
        throw new UnsupportedOperationException();
    }

    @Override
    public void forEachEntry(Consumer<? super MapKeyContext<K, V>> action) {
        // TODO implement!
        throw new UnsupportedOperationException();
    }


    public String serverApplicationVersion() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        // todo add ref count
    }

    @Override
    public Class<V> valueClass() {
        return vClass;
    }


    @NotNull
    public File file() {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("NullableProblems")
    public V putIfAbsent(K key, V value) {

        if (key == null || value == null)
            throw new NullPointerException();

        return (V) proxyReturnTypedObject(putIfAbsent, vClass, key, value);
    }

    @SuppressWarnings("NullableProblems")
    public boolean remove(Object key, Object value) {
        if (key == null)
            throw new NullPointerException();

        return value != null && proxyReturnBoolean(removeWithValue, (K) key, (V) value);
    }

    @SuppressWarnings("NullableProblems")
    public boolean replace(K key, V oldValue, V newValue) {
        if (key == null || oldValue == null || newValue == null)
            throw new NullPointerException();
        return proxyReturnBoolean(replaceWithOldAndNewValue, key, oldValue, newValue);
    }

    @SuppressWarnings("NullableProblems")
    public V replace(K key, V value) {
        if (key == null || value == null)
            throw new NullPointerException();
        return (V) proxyReturnTypedObject(replace, vClass, key, value);
    }

    public int size() {
        return (int) longSize();
    }

    /**
     * calling this method should be avoided at all cost, as the entire {@code object} is
     * serialized. This equals can be used to compare map that extends ChronicleMap.  So two
     * Chronicle Maps that contain the same data are considered equal, even if the instances of the
     * chronicle maps were of different types
     *
     * @param object the object that you are comparing against
     * @return true if the contain the same data
     */
    @Override
    public boolean equals(@Nullable Object object) {
        if (this == object) return true;
        if (object == null || object.getClass().isAssignableFrom(Map.class))
            return false;

        final Map<? extends K, ? extends V> that = (Map<? extends K, ? extends V>) object;

        final int size = size();

        if (that.size() != size)
            return false;

        final Set<Map.Entry<K, V>> entries = entrySet();
        return that.entrySet().equals(entries);
    }

    @Override
    public int hashCode() {
        return proxyReturnInt(hashCode);
    }

    @NotNull
    public String toString() {

        return "";
    }

    @NotNull
    public String serverPersistedDataVersion() {
        return hub.proxyReturnString(persistedDataVersion, csp, 0);
    }

    public boolean isEmpty() {
        return proxyReturnBoolean(isEmpty);
    }

    public boolean containsKey(Object key) {
        return proxyReturnBooleanK(containsKey, (K) key);
    }

    @NotNull
    private NullPointerException keyNotNullNPE() {
        return new NullPointerException("key can not be null");
    }

    public boolean containsValue(Object value) {
        return proxyReturnBooleanV(containsValue, (V) value);
    }

    public long longSize() {
        return proxyReturnLong(longSize);
    }

    @Override
    public MapKeyContext<K, V> context(K key) {
        throw new UnsupportedOperationException("Contexts are not supported by stateless clients");
    }

    public V get(Object key) {
        return (V) proxyReturnObject(vClass, get, (K) key);
    }

    @Nullable
    public V getUsing(K key, V usingValue) {
        throw new UnsupportedOperationException();
    }

    @NotNull
    public V acquireUsing(@NotNull K key, V usingValue) {
        throw new UnsupportedOperationException(
                "acquireUsing() is not supported for stateless clients");
    }

    @NotNull
    @Override
    public MapKeyContext<K, V> acquireContext(@NotNull K key, @NotNull V usingValue) {
        throw new UnsupportedOperationException("Contexts are not supported by stateless clients");
    }

    public V remove(Object key) {
        if (key == null)
            throw keyNotNullNPE();
        return (V) proxyReturnObject(vClass, removeReturnsNull ? removeWithoutAcc : remove, key);
    }

    @Override
    public void createChannel(short channelID) {
        proxyReturnVoid(createChannel, outValue -> outValue.int16(channelID));

    }

    public void putAll(@NotNull Map<? extends K, ? extends V> map) {


    }


    public V put(K key, V value) {
        if (key == null || value == null)
            throw new NullPointerException();
        return (V) proxyReturnTypedObject(putReturnsNull ? put : getAndPut,
                vClass,
                key,
                value);
    }


    @Nullable
    public <R> R getMapped(@Nullable K key, @NotNull SerializableFunction<? super V, R> function) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public V putMapped(@Nullable K key, @NotNull UnaryOperator<V> unaryOperator) {
        throw new UnsupportedOperationException();
    }

    public void clear() {
        proxyReturnVoid(clear);
    }

    @NotNull
    @Override
    public Set<K> keySet() {
        return null;
    }

    @NotNull
    @Override
    public Collection<V> values() {
        return null;
    }


    private final Map<Long, String> cidToCsp = new HashMap<>();

    @NotNull
    public Set<Map.Entry<K, V>> entrySet() {

        final long[] cidRef = new long[1];

        proxyReturnWireConsumer(entrySet, (WireIn wireIn) -> {
            final StringBuilder type = Wires.acquireStringBuilder();
            final ValueIn read = wireIn.read(reply);
            read.type(type);
            read.marshallable(w -> {

                final String csp1 = w.read(CoreFields.csp).text();
                final long cid = w.read(CoreFields.cid).int64();
                cidToCsp.put(cid, csp1);
                cidRef[0] = cid;

            });
        });

        return new ClientWiredStatelessChronicleEntrySet<K, V>(channelName, hub, cidRef[0], vClass);
    }


/*    */

    /**
     * @param callback each entry is passed to the callback
     *//*
    void entrySet(@NotNull MapEntryCallback<K, V> callback) {
        throw new UnsupportedOperationException();
    }*/
    private K readK(WireKey argName, Wire wireIn, K usingValue) {
        return (K) readObject(argName, wireIn, usingValue, kClass);
    }


    private int readInt(long tid, long startTime) {
        assert !hub.outBytesLock().isHeldByCurrentThread();

        long timeoutTime = startTime + hub.timeoutMs;

        // receive
        hub.inBytesLock().lock();
        try {
            final Wire wireIn = hub.proxyReply(timeoutTime, tid);
            checkIsData(wireIn);
            return wireIn.read(reply).int32();
        } finally {
            hub.inBytesLock().unlock();
        }
    }


    @SuppressWarnings("SameParameterValue")
    private boolean proxyReturnBooleanV(@NotNull final EventId eventId, V value) {
        final long startTime = System.currentTimeMillis();
        return readBoolean(sendEvent(startTime, eventId, out -> writeField(out, value)),
                startTime);
    }

    @SuppressWarnings("SameParameterValue")
    private boolean proxyReturnBooleanK(@NotNull final EventId eventId, K key) {
        final long startTime = System.currentTimeMillis();
        return readBoolean(sendEvent(startTime, eventId, out -> writeField(out, key)), startTime);
    }


    @SuppressWarnings("SameParameterValue")
    private boolean proxyReturnBoolean(@NotNull final EventId eventId) {
        final long startTime = System.currentTimeMillis();
        return readBoolean(sendEvent(startTime, eventId, VOID_PARAMETERS), startTime);
    }


    @SuppressWarnings("SameParameterValue")
    private int proxyReturnInt(@NotNull final EventId eventId) {
        final long startTime = System.currentTimeMillis();
        return readInt(sendEvent(startTime, eventId, VOID_PARAMETERS), startTime);
    }

/*
    private long proxySend(EventId methodName, long startTime) {
        long tid;
        hub.outBytesLock().lock();
        try {
            tid = writeHeader(startTime);
            hub.outWire().writeDocument(false, wireOut -> wireOut.writeEventName(methodName));
            hub.writeSocket(hub.outWire());
        } finally {
            hub.outBytesLock().unlock();
        }
        return tid;
    }*/


    @Override
    protected boolean eventReturnsNull(@NotNull EventId methodName) {

        switch (methodName) {
            case putAllWithoutAcc:
            case put:
            case removeWithoutAcc:
                return true;
            default:
                return false;
        }

    }


    class Entry implements Map.Entry<K, V> {

        final K key;
        final V value;

        /**
         * Creates new entry.
         */
        Entry(K k1, V v) {
            value = v;
            key = k1;
        }

        public final K getKey() {
            return key;
        }

        public final V getValue() {
            return value;
        }

        public final V setValue(V newValue) {
            V oldValue = value;
            ClientWiredStatelessChronicleMap.this.put(getKey(), newValue);
            return oldValue;
        }

        public final boolean equals(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            final Map.Entry e = (Map.Entry) o;
            final Object k1 = getKey();
            final Object k2 = e.getKey();
            if (k1 == k2 || (k1 != null && k1.equals(k2))) {
                Object v1 = getValue();
                Object v2 = e.getValue();
                if (v1 == v2 || (v1 != null && v1.equals(v2)))
                    return true;
            }
            return false;
        }

        public final int hashCode() {
            return (key == null ? 0 : key.hashCode()) ^
                    (value == null ? 0 : value.hashCode());
        }

        @NotNull
        public final String toString() {
            return getKey() + "=" + getValue();
        }
    }

    @Override
    protected Consumer<ValueOut> toParameters(@NotNull EventId eventId, Object... args) {

        return out -> {
            final MapWireHandler.Params[] paramNames = eventId.params();

            if (paramNames.length == 1) {
                writeField(out, args[0]);
                return;
            }

            assert args.length == paramNames.length :
                    "methodName=" + eventId +
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


}

