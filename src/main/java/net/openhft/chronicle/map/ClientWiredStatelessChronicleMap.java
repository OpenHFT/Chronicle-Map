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

import net.openhft.chronicle.hash.function.SerializableFunction;
import net.openhft.chronicle.map.MapWireHandlerBuilder.Fields;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static java.util.Collections.emptyList;
import static net.openhft.chronicle.map.ClientWiredStatelessChronicleMap.EventId.*;


/**
 * @author Rob Austin.
 */
class ClientWiredStatelessChronicleMap<K, V> implements ChronicleMap<K, V>, Cloneable, ChannelFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ClientWiredStatelessChronicleMap.class);

    private final ClientWiredStatelessClientTcpConnectionHub hub;

    protected Class<K> kClass;
    protected Class<V> vClass;

    private boolean putReturnsNull;
    private boolean removeReturnsNull;
    private short channelID;

    public ClientWiredStatelessChronicleMap(@NotNull final ClientWiredChronicleMapStatelessClientBuilder config,
                                            @NotNull final Class kClass,
                                            @NotNull final Class vClass,
                                            short channelID) {
        this.channelID = channelID;
        hub = config.hub;
        this.putReturnsNull = config.putReturnsNull();
        this.removeReturnsNull = config.removeReturnsNull();
        this.kClass = kClass;
        this.vClass = vClass;
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
        return VanillaChronicleMap.newInstance(vClass, false);
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
        return hub.serverApplicationVersion(channelID);
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

        return proxyReturnObject(PUT_IF_ABSENT.toString(), key, value, vClass);
    }

    @SuppressWarnings("NullableProblems")
    public boolean remove(Object key, Object value) {

        if (key == null)
            throw new NullPointerException();

        return value != null && proxyReturnBoolean(REMOVE_WITH_VALUE.toString(), (K) key, (V) value);

    }

    @SuppressWarnings("NullableProblems")
    public boolean replace(K key, V oldValue, V newValue) {
        if (key == null || oldValue == null || newValue == null)
            throw new NullPointerException();
        return proxyReturnBoolean(REPLACE_WITH_OLD_AND_NEW_VALUE.toString(), key, oldValue, newValue);
    }

    @SuppressWarnings("NullableProblems")
    public V replace(K key, V value) {
        if (key == null || value == null)
            throw new NullPointerException();
        return proxyReturnObject(REPLACE.toString(), key, value, vClass);
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
        return proxyReturnInt(HASH_CODE.toString());
    }

    @NotNull
    public String toString() {
        return hub.proxyReturnString(TO_STRING.toString(), channelID);
    }

    @NotNull
    public String serverPersistedDataVersion() {
        return hub.proxyReturnString(PERSISTED_DATA_VERSION.toString(), channelID);
    }

    public boolean isEmpty() {
        return proxyReturnBoolean(IS_EMPTY.toString());
    }

    public boolean containsKey(Object key) {
        return proxyReturnBooleanK(CONTAINS_KEY.toString(), (K) key);
    }

    @NotNull
    private NullPointerException keyNotNullNPE() {
        return new NullPointerException("key can not be null");
    }

    public boolean containsValue(Object value) {
        return proxyReturnBooleanV(CONTAINS_VALUE.toString(), (V) value);
    }

    public long longSize() {
        return proxyReturnLong(LONG_SIZE.toString());
    }

    @Override
    public MapKeyContext<K, V> context(K key) {
        throw new UnsupportedOperationException("Contexts are not supported by stateless clients");
    }

    public V get(Object key) {
        return proxyReturnObject(vClass, GET.toString(), (K) key);
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
        return proxyReturnObject(vClass, removeReturnsNull ? REMOVE_WITHOUT_ACC.toString() : REMOVE.toString(), (K) key);
    }

    @Override
    public void createChannel(short channelID) {
        proxyReturnVoid(
                () -> {
                    writeField(Fields.METHOD_NAME, "CREATE_CHANNEL");
                    hub.outWire().write(Fields.ARG_1).int16(channelID);
                });
    }

    public void putAll(@NotNull Map<? extends K, ? extends V> map) {

        final long startTime = System.currentTimeMillis();
        long transactionId = hub.nextUniqueTransaction(startTime);


        Set<? extends Map.Entry<? extends K, ? extends V>> entries = map.entrySet();
        Iterator<? extends Map.Entry<? extends K, ? extends V>> iterator = entries.iterator();

        boolean hasNext;

        do {

            hasNext = iterator.hasNext();

            hub.outBytesLock().lock();
            try {
                assert hub.outBytesLock().isHeldByCurrentThread();
                assert !hub.inBytesLock().isHeldByCurrentThread();

                Wire wire = hub.outWire();

                assert hub.outBytesLock().isHeldByCurrentThread();
                hub.markSize(wire);
                hub.startTime(startTime);

                wire.write(Fields.TRANSACTION_ID).int64(transactionId);
                wire.write(Fields.TIME_STAMP).int64(startTime);
                wire.write(Fields.CHANNEL_ID).int16(channelID);

                writeField(Fields.METHOD_NAME, "PUT_ALL");

                hub.outWire().write(Fields.HAS_NEXT).bool(hasNext);

                if (hasNext) {
                    Map.Entry<? extends K, ? extends V> e = iterator.next();
                    writeField(Fields.ARG_1, e.getKey());
                    writeField(Fields.ARG_2, e.getValue());
                }

                hub.writeSocket(hub.outWire());

            } finally {
                hub.outBytesLock().unlock();
            }

        } while (hasNext);

        // wait for the transaction id to be received, this will only be received once the
        // last chunk has been processed
        readVoid(transactionId, startTime);
    }


    public V put(K key, V value) {
        if (key == null || value == null)
            throw new NullPointerException();
        return proxyReturnObject(putReturnsNull ? PUT_WITHOUT_ACC.toString() : PUT.toString(), key, value, vClass);
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
        proxyReturnVoid(() -> writeField(Fields.METHOD_NAME, CLEAR.toString()));
    }


    public Set<K> keySet() {
        final long startTime = System.currentTimeMillis();
        final Set<K> result = new HashSet<K>();
        // send
        final long transactionId = proxySend("KEY_SET", startTime);
        assert !hub.outBytesLock().isHeldByCurrentThread();
        final long timeoutTime = startTime + hub.timeoutMs;

        for (; ; ) {

            // receive
            hub.inBytesLock().lock();
            try {
                final Wire wireIn = hub.proxyReply(timeoutTime, transactionId);


                if (wireIn.read(Fields.HAS_NEXT).bool()) {
                    K k = readK(Fields.RESULT, wireIn, null);
                    result.add(k);
                } else
                    break;

            } finally {
                hub.inBytesLock().unlock();
            }
        }
        return result;
    }


    public Collection<V> values() {
        final long startTime = System.currentTimeMillis();
        final Set<V> result = new HashSet<V>();
        // send
        final long transactionId = proxySend("VALUES", startTime);
        assert !hub.outBytesLock().isHeldByCurrentThread();
        final long timeoutTime = startTime + hub.timeoutMs;

        for (; ; ) {

            // receive
            hub.inBytesLock().lock();
            try {
                final Wire wireIn = hub.proxyReply(timeoutTime, transactionId);


                if (wireIn.read(Fields.HAS_NEXT).bool()) {
                    V v = readV(Fields.RESULT, wireIn, null);
                    result.add(v);
                } else
                    break;

            } finally {
                hub.inBytesLock().unlock();
            }
        }
        return result;
    }

    @NotNull
    public Set<Map.Entry<K, V>> entrySet() {
        final long startTime = System.currentTimeMillis();
        final Map<K, V> result = new HashMap<K, V>();

        // send
        final long transactionId = proxySend("ENTRY_SET", startTime);
        assert !hub.outBytesLock().isHeldByCurrentThread();
        final long timeoutTime = startTime + hub.timeoutMs;

        for (; ; ) {

            // receive
            hub.inBytesLock().lock();
            try {
                final Wire wireIn = hub.proxyReply(timeoutTime, transactionId);

                if (wireIn.read(Fields.HAS_NEXT).bool()) {
                    result.put(
                            readK(Fields.RESULT_KEY, wireIn, null),
                            readV(Fields.RESULT_VALUE, wireIn, null));
                } else
                    break;

            } finally {
                hub.inBytesLock().unlock();
            }
        }
        return result.entrySet();
    }

    /**
     * @param callback each entry is passed to the callback
     */
    void entrySet(@NotNull MapEntryCallback<K, V> callback) {
        throw new UnsupportedOperationException();
    }


    private long readLong(long transactionId, long startTime) {
        assert !hub.outBytesLock().isHeldByCurrentThread();
        final long timeoutTime = startTime + hub.timeoutMs;

        // receive
        hub.inBytesLock().lock();
        try {
            return hub.proxyReply(timeoutTime, transactionId).read(Fields.RESULT).int64();
        } finally {
            hub.inBytesLock().unlock();
        }
    }


    private void writeField(Fields fieldName, Object value) {
        writeField(fieldName, value, hub.outWire());
    }

    private void writeField(Fields fieldName, Object value, Wire wire) {

        assert hub.outBytesLock().isHeldByCurrentThread();
        assert !hub.inBytesLock().isHeldByCurrentThread();

        if (value instanceof Byte)
            wire.write(fieldName).int8((Byte) value);
        else if (value instanceof Character)
            wire.write(fieldName).text(((Character) value).toString());
        else if (value instanceof Short)
            wire.write(fieldName).int16((Short) value);
        else if (value instanceof Integer)
            wire.write(fieldName).int32((Integer) value);
        else if (value instanceof Long)
            wire.write(fieldName).int64((Long) value);
        else if (value instanceof CharSequence) {
            wire.write(fieldName).text((CharSequence) value);
        } else if (value instanceof Marshallable) {
            wire.write(fieldName).marshallable((Marshallable) value);
        } else {
            throw new IllegalStateException("type=" + value.getClass() + " is unsupported, it must either be of type Marshallable or CharSequence");
        }
    }


    private V readValue(Fields argName, long transactionId, long startTime, final V usingValue) {
        assert !hub.outBytesLock().isHeldByCurrentThread();
        long timeoutTime = startTime + hub.timeoutMs;

        hub.inBytesLock().lock();
        try {

            final Wire wireIn = hub.proxyReply(timeoutTime, transactionId);

            if (wireIn.read(Fields.RESULT_IS_NULL).bool())
                return null;

            return readV(argName, wireIn, usingValue);

        } finally {
            hub.inBytesLock().unlock();
        }
    }

    private V readV(Fields argName, Wire wireIn, V usingValue) {
        if (StringBuilder.class.isAssignableFrom(vClass)) {
            wireIn.read(argName).text((StringBuilder) usingValue);
            return usingValue;
        } else if (Marshallable.class.isAssignableFrom(vClass)) {

            if (usingValue == null)
                try {
                    V v = vClass.newInstance();
                    wireIn.read(argName).marshallable((Marshallable) v);
                    return v;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

            wireIn.read(argName).marshallable((Marshallable) usingValue);
            return usingValue;

        } else if (String.class.isAssignableFrom(vClass)) {
            //noinspection unchecked
            return (V) wireIn.read(argName).text();

        } else if (Long.class.isAssignableFrom(vClass)) {
            //noinspection unchecked
            return (V) (Long) wireIn.read(argName).int64();
        } else if (Double.class.isAssignableFrom(vClass)) {
            //noinspection unchecked
            return (V) (Double) wireIn.read(argName).float64();

        } else if (Integer.class.isAssignableFrom(vClass)) {
            //noinspection unchecked
            return (V) (Integer) wireIn.read(argName).int32();

        } else if (Float.class.isAssignableFrom(vClass)) {
            //noinspection unchecked
            return (V) (Float) wireIn.read(argName).float32();

        } else if (Short.class.isAssignableFrom(vClass)) {
            //noinspection unchecked
            return (V) (Short) wireIn.read(argName).int16();

        } else if (Character.class.isAssignableFrom(vClass)) {
            //noinspection unchecked
            final String text = wireIn.read(argName).text();
            if (text == null || text.length() == 0)
                return null;
            return (V) (Character) text.charAt(0);

        } else if (Byte.class.isAssignableFrom(vClass)) {
            //noinspection unchecked
            return (V) (Byte) wireIn.read(argName).int8();


        } else {
            throw new IllegalStateException("unsupported type");
        }
    }

    private K readK(Fields argName, Wire wireIn, K usingValue) {
        if (StringBuilder.class.isAssignableFrom(kClass)) {
            wireIn.read(argName).text((StringBuilder) usingValue);
            return usingValue;
        } else if (Marshallable.class.isAssignableFrom(kClass)) {

            if (usingValue == null)
                try {
                    K v = kClass.newInstance();
                    wireIn.read(argName).marshallable((Marshallable) v);
                    return v;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

            wireIn.read(argName).marshallable((Marshallable) usingValue);
            return usingValue;

        } else if (String.class.isAssignableFrom(vClass)) {
            //noinspection unchecked
            return (K) wireIn.read(argName).text();

        } else {
            throw new IllegalStateException("unsupported type");
        }
    }

    private boolean readBoolean(long transactionId, long startTime) {
        assert !hub.outBytesLock().isHeldByCurrentThread();

        long timeoutTime = startTime + hub.timeoutMs;

        // receive
        hub.inBytesLock().lock();
        try {
            final Wire wireIn = hub.proxyReply(timeoutTime, transactionId);
            return wireIn.read(Fields.RESULT).bool();
        } finally {
            hub.inBytesLock().unlock();
        }
    }

    private int readInt(long transactionId, long startTime) {
        assert !hub.outBytesLock().isHeldByCurrentThread();

        long timeoutTime = startTime + hub.timeoutMs;

        // receive
        hub.inBytesLock().lock();
        try {
            final Wire wireIn = hub.proxyReply(timeoutTime, transactionId);
            return wireIn.read(Fields.RESULT).int32();
        } finally {
            hub.inBytesLock().unlock();
        }
    }

    @SuppressWarnings("SameParameterValue")
    private boolean proxyReturnBoolean(@NotNull final String methodName, K key, V value) {
        final long startTime = System.currentTimeMillis();

        long transactionId;

        hub.outBytesLock().lock();
        try {
            transactionId = hub.writeHeader(startTime, channelID, hub.outWire());
            writeField(Fields.METHOD_NAME, methodName);
            writeField(Fields.ARG_1, key);
            writeField(Fields.ARG_2, value);

            hub.writeSocket(hub.outWire());
        } finally {
            hub.outBytesLock().unlock();
        }

        return readBoolean(transactionId, startTime);

    }

    @SuppressWarnings("SameParameterValue")
    private boolean proxyReturnBoolean(@NotNull final String methodName, K key, V value1, V value2) {
        final long startTime = System.currentTimeMillis();

        long transactionId;
        hub.outBytesLock().lock();
        try {
            transactionId = hub.writeHeader(startTime, channelID, hub.outWire());
            writeField(Fields.METHOD_NAME, methodName);
            writeField(Fields.ARG_1, value1);
            writeField(Fields.ARG_2, value2);
            hub.writeSocket(hub.outWire());
        } finally {
            hub.outBytesLock().unlock();
        }

        return readBoolean(transactionId, startTime);

    }

    @SuppressWarnings("SameParameterValue")
    private boolean proxyReturnBooleanV(@NotNull final String methodName, V value) {
        final long startTime = System.currentTimeMillis();
        return readBoolean(proxySend(methodName, startTime, value), startTime);
    }

    @SuppressWarnings("SameParameterValue")
    private boolean proxyReturnBooleanK(@NotNull final String methodName, K key) {
        final long startTime = System.currentTimeMillis();

        return readBoolean(proxySend(methodName, startTime, key), startTime);
    }

    @SuppressWarnings("SameParameterValue")
    private long proxyReturnLong(@NotNull final String methodName) {
        final long startTime = System.currentTimeMillis();

        long transactionId;

        hub.outBytesLock().lock();
        try {
            transactionId = hub.writeHeader(startTime, channelID, hub.outWire());
            writeField(Fields.METHOD_NAME, methodName);
            hub.writeSocket(hub.outWire());
        } finally {
            hub.outBytesLock().unlock();
        }

        return readLong(transactionId, startTime);
    }

    @SuppressWarnings("SameParameterValue")
    private boolean proxyReturnBoolean(@NotNull final String methodName) {
        final long startTime = System.currentTimeMillis();

        long transactionId;

        hub.outBytesLock().lock();
        try {
            transactionId = hub.writeHeader(startTime, channelID, hub.outWire());
            writeField(Fields.METHOD_NAME, methodName);
            hub.writeSocket(hub.outWire());
        } finally {
            hub.outBytesLock().unlock();
        }

        return readBoolean(transactionId, startTime);
    }

    @SuppressWarnings("SameParameterValue")
    private void proxyReturnVoid(Runnable r) {


        final long startTime = System.currentTimeMillis();
        long transactionId;

        hub.outBytesLock().lock();
        try {
            transactionId = hub.writeHeader(startTime, channelID, hub.outWire());
            r.run();
            hub.writeSocket(hub.outWire());
        } finally {
            hub.outBytesLock().unlock();
        }

        readVoid(transactionId, startTime);
    }


    @SuppressWarnings("SameParameterValue")
    private int proxyReturnInt(@NotNull final String methodName) {
        final long startTime = System.currentTimeMillis();

        long transactionId;

        hub.outBytesLock().lock();
        try {
            transactionId = hub.writeHeader(startTime, channelID, hub.outWire());
            writeField(Fields.METHOD_NAME, methodName);
            hub.writeSocket(hub.outWire());
        } finally {
            hub.outBytesLock().unlock();
        }

        return readInt(transactionId, startTime);
    }


    @Nullable
    private <R> R proxyReturnObject(@NotNull final String methodName, K key, V value, Class<V> resultType) {

        final long startTime = System.currentTimeMillis();
        long transactionId;

        hub.outBytesLock().lock();
        try {
            assert hub.outBytesLock().isHeldByCurrentThread();
            assert !hub.inBytesLock().isHeldByCurrentThread();

            transactionId = hub.writeHeader(startTime, channelID, hub.outWire());
            writeField(Fields.METHOD_NAME, methodName);
            writeField(Fields.ARG_1, key);
            writeField(Fields.ARG_2, value);
            hub.writeSocket(hub.outWire());

        } finally {
            hub.outBytesLock().unlock();
        }

        if (eventReturnsNull(methodName))
            return null;

        if (resultType == vClass)
            return (R) readValue(Fields.RESULT, transactionId, startTime, null);

        else
            throw new UnsupportedOperationException("class of type class=" + resultType + " is not " +
                    "supported");
    }

    @Nullable
    private <R> R proxyReturnObject(Class<R> rClass, @NotNull final String methodName, Object key) {
        final long startTime = System.currentTimeMillis();

        long transactionId;

        transactionId = proxySend(methodName, startTime, key);

        if (eventReturnsNull(methodName))
            return null;

        if (rClass == vClass)
            return (R) readValue(Fields.RESULT, transactionId, startTime, null);
        else
            throw new UnsupportedOperationException("class of type class=" + rClass + " is not " +
                    "supported");
    }

    private long proxySend(String methodName, long startTime, Object arg1) {
        long transactionId;
        hub.outBytesLock().lock();
        try {
            transactionId = hub.writeHeader(startTime, channelID, hub.outWire());
            writeField(Fields.METHOD_NAME, methodName);
            writeField(Fields.ARG_1, arg1);
            hub.writeSocket(hub.outWire());
        } finally {
            hub.outBytesLock().unlock();
        }
        return transactionId;
    }


    private long proxySend(String methodName, long startTime) {
        long transactionId;
        hub.outBytesLock().lock();
        try {
            transactionId = hub.writeHeader(startTime, channelID, hub.outWire());
            writeField(Fields.METHOD_NAME, methodName);
            hub.writeSocket(hub.outWire());
        } finally {
            hub.outBytesLock().unlock();
        }
        return transactionId;
    }

    private boolean eventReturnsNull(@NotNull String methodName) {

        switch (methodName) {
            case "PUT_ALL_WITHOUT_ACC":
            case "PUT_WITHOUT_ACC":
            case "REMOVE_WITHOUT_ACC":
                return true;
            default:
                return false;
        }

    }

    private void readVoid(long transactionId, long startTime) {
        long timeoutTime = startTime + hub.timeoutMs;

        // receive
        hub.inBytesLock().lock();
        try {
            hub.proxyReply(timeoutTime, transactionId);
        } finally {
            hub.inBytesLock().unlock();
        }
    }


    static enum EventId {
        HEARTBEAT,
        STATEFUL_UPDATE,
        LONG_SIZE,
        SIZE,
        IS_EMPTY,
        CONTAINS_KEY,
        CONTAINS_VALUE,
        GET,
        PUT,
        PUT_WITHOUT_ACC,
        REMOVE,
        REMOVE_WITHOUT_ACC,
        CLEAR,
        KEY_SET,
        VALUES,
        ENTRY_SET,
        REPLACE,
        REPLACE_WITH_OLD_AND_NEW_VALUE,
        PUT_IF_ABSENT,
        REMOVE_WITH_VALUE,
        TO_STRING,
        APPLICATION_VERSION,
        PERSISTED_DATA_VERSION,
        PUT_ALL,
        PUT_ALL_WITHOUT_ACC,
        HASH_CODE,
        MAP_FOR_KEY,
        PUT_MAPPED,
        KEY_BUILDER,
        VALUE_BUILDER
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
}

