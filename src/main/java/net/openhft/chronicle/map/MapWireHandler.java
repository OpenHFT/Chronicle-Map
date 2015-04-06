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

/**
 * Created by Rob Austin
 */

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.ChronicleHashInstanceBuilder;
import net.openhft.chronicle.hash.impl.util.BuildVersion;
import net.openhft.chronicle.hash.replication.ReplicationHub;
import net.openhft.chronicle.map.MapWireHandlerBuilder.Fields;
import net.openhft.chronicle.network2.WireHandler;
import net.openhft.chronicle.network2.event.EventGroup;
import net.openhft.chronicle.network2.event.WireHandlers;
import net.openhft.chronicle.wire.*;
import net.openhft.lang.io.DirectStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StreamCorruptedException;
import java.io.StringWriter;
import java.nio.BufferOverflowException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static net.openhft.chronicle.map.MapWireHandler.EventId.*;
import static net.openhft.chronicle.map.MapWireHandler.Params.*;
import static net.openhft.chronicle.map.MapWireHandlerBuilder.Fields.*;

/**
 * @author Rob Austin.
 */
class MapWireHandler<K, V> implements WireHandler, Consumer<WireHandlers> {


    public static final int SIZE_OF_SIZE = 2;
    private static final Logger LOG = LoggerFactory.getLogger(MapWireHandler.class);
    final Map<Long, Runnable> incompleteWork = new HashMap<Long, Runnable>();
    private final ArrayList<BytesChronicleMap> bytesChronicleMaps = new ArrayList<>();
    @NotNull

    private final Supplier<ChronicleHashInstanceBuilder<ChronicleMap<K, V>>> mapFactory;
    private Wire inWire = null;
    private Wire outWire = null;
    private final Consumer writeElement = new Consumer<Iterator<byte[]>>() {

        @Override
        public void accept(Iterator<byte[]> iterator) {
            outWire.write(reply);
            Bytes<?> bytes = outWire.bytes();
            bytes.write(iterator.next());
        }
    };
    private final Consumer writeEntry = new Consumer<Iterator<Map.Entry<byte[], byte[]>>>() {

        @Override
        public void accept(Iterator<Map.Entry<byte[], byte[]>> iterator) {

            final Map.Entry<byte[], byte[]> entry = iterator.next();

            outWire.write(resultKey);
            Bytes<?> bytes = outWire.bytes();
            bytes.write(entry.getKey());

            outWire.write(resultValue);
            Bytes<?> bytes1 = outWire.bytes();
            bytes1.write(entry.getValue());
        }
    };
    private WireHandlers publishLater;
    private byte localIdentifier;
    private long timestamp;
    private short channelId;
    private List<Replica> channels;
    private ReplicationHub hub;
    private byte remoteIdentifier;

    public MapWireHandler(
            @NotNull Supplier<ChronicleHashInstanceBuilder<ChronicleMap<K, V>>> mapFactory,
            @NotNull ReplicationHub hub, byte localIdentifier, @NotNull List<Replica> channels) {
        this(mapFactory, hub);
        this.channels = channels;
        this.localIdentifier = localIdentifier;
    }

    public MapWireHandler(Supplier<ChronicleHashInstanceBuilder<ChronicleMap<K, V>>> mapFactory,
                          ReplicationHub hub) {
        this.mapFactory = mapFactory;
        this.hub = hub;
    }

    @Override
    public void accept(WireHandlers wireHandlers) {
        this.publishLater = wireHandlers;
    }

    /**
     * @param tid the transaction id
     * @param function      provides that returns items bases on a BytesChronicleMap
     * @param c             an iterator that contains items
     * @param maxEntries    the maximum number of items that can be written
     * @throws StreamCorruptedException
     */
    private void writeChunked(
            long tid, @NotNull final Function<BytesChronicleMap, Iterator> function,
            @NotNull final Consumer<Iterator> c, long maxEntries) throws StreamCorruptedException {

        final BytesChronicleMap m = bytesMap(channelId);
        final Iterator iterator = function.apply(m);

        final WireHandler that = new WireHandler() {

            @Override
            public void process(Wire in, Wire out) throws StreamCorruptedException {

                outWire.write(Fields.tid).int64(tid);

                // this allows us to write more data than the buffer will allow
                for (int count = 0; ; count++) {

                    boolean finished = count == maxEntries;

                    final boolean hasNext = iterator.hasNext() && !finished;

                    write(map -> {

                        outWire.write(Fields.hasNext).bool(hasNext);

                        if (hasNext) {
                            c.accept(iterator);
                        }

                    });

                    if (!hasNext)
                        return;

                    // quit if we have filled the buffer
                    Bytes<?> bytes = outWire.bytes();
                    if (bytes.remaining() < (bytes.capacity() * 0.75)) {
                        publishLater.add(this);
                        return;
                    }


                }
            }
        };

        that.process(inWire, outWire);

    }

    private void writeChunked(
            long tid,
            @NotNull final Function<BytesChronicleMap, Iterator> function,
            @NotNull final Consumer<Iterator> c) throws StreamCorruptedException {
        writeChunked(tid, function, c, Long.MAX_VALUE);
    }


    @Override
    public void process(Wire in, Wire out) throws StreamCorruptedException {
        try {
            this.inWire = in;
            this.outWire = out;
            onEvent();
        } catch (Exception e) {
            LOG.error("", e);
        }
    }


    @SuppressWarnings("UnusedReturnValue")
    void onEvent() throws StreamCorruptedException {

        // it is assumed by this point that the buffer has all the bytes in it for this message

        final long tid = inWire.read(Fields.tid).int64();
        timestamp = inWire.read(timeStamp).int64();
        channelId = inWire.read(Fields.channelId).int16();

        final Bytes<?> bytes1 = inWire.bytes();

        final int body = bytes1.readVolatileInt();
        assert Wires.isData(body);

        final StringBuilder eventName = Wires.acquireStringBuilder();
        final ValueIn valueIn = inWire.readEventName(eventName);

        if (!incompleteWork.isEmpty()) {
            Runnable runnable = incompleteWork.get(tid);
            if (runnable != null) {
                runnable.run();
                return;
            }
        }

        final Bytes<?> outBytes = outWire.bytes();
        final Bytes<?> bytes = outBytes;

        try {

            if (putWithoutAcc.contentEquals(eventName)) {

                valueIn.marshallable(wire -> {

                    final Params[] params = putIfAbsent.params;
                    final byte[] key = wire.read(params[0]).bytes();
                    final byte[] value = wire.read(params[1]).bytes();

                    writeVoid(b -> ((Map<byte[], byte[]>) b.delegate).putIfAbsent(key, value));

                });

                return;
            }


            if (keySet.contentEquals(eventName)) {
                writeChunked(tid, map -> map.keySet().iterator(), writeElement);
                return;
            }

            if (values.contentEquals(eventName)) {
                writeChunked(tid, map -> map.delegate.values().iterator(), writeElement);
                return;
            }

            if (entrySet.contentEquals(eventName)) {
                writeChunked(tid, m -> m.delegate.entrySet().iterator(), writeEntry);
                return;
            }

            if (entrySetRestricted.contentEquals(eventName)) {
                long maxEntries = inWire.read(arg1).int64();
                writeChunked(tid, m -> m.delegate.entrySet().iterator(), writeEntry,maxEntries);
                return;
            }

            if (putAll.contentEquals(eventName)) {
                putAll(tid);
                return;
            }

            outWire.writeDocument(true, wire -> outWire.write(Fields.tid).int64(tid));

            // write the transaction id
            if (createChannel.contentEquals(eventName)) {
                writeVoid(() -> {

                    short channelId1 = valueIn.int16();
                    mapFactory.get().replicatedViaChannel(hub.createChannel(channelId1)).create();
                    return null;
                });
                return;
            }

            if (EventId.remoteIdentifier.contentEquals(eventName)) {
                this.remoteIdentifier = valueIn.int8();
                return;
            }

            if (longSize.contentEquals(eventName)) {
                write(b -> outWire.write(reply).int64(b.longSize()));
                return;
            }

            if (isEmpty.contentEquals(eventName)) {
                write(b -> outWire.write(reply).bool(b.isEmpty()));
                return;
            }

            if (containsKey.contentEquals(eventName)) {
                // todo remove the    toByteArray(..)
                write(b -> outWire.write(reply)
                        .bool(b.delegate.containsKey(toByteArray(valueIn))));
                return;
            }

            if (containsValue.contentEquals(eventName)) {
                // todo remove the    toByteArray(..)
                write(b -> outWire.write(reply)
                        .bool(b.delegate.containsValue(toByteArray(valueIn))));
                return;
            }

            if (get.contentEquals(eventName)) {
                // todo remove the    toByteArray(..)
                writeValueUsingDelegate(map -> {
                    final byte[] key = toByteArray(valueIn);
                    final byte[] value = map.get(key);
                    return value;
                });
                return;
            }

            if (put.contentEquals(eventName)) {

                valueIn.marshallable(wire -> {

                    final Params[] params = put.params;
                    final byte[] key1 = wire.read(params[0]).bytes();

                    final byte[] value1 = wire.read(params[1]).bytes();

                    MapWireHandler.this.writeValue(b -> {
                        final byte[] result = b.put(key1, value1);

                        System.out.println("key='" + new String(b.keySet().iterator().next()) + "'");
                        return result;
                    });

                });

                return;
            }

            if (remove.contentEquals(eventName)) {

                MapWireHandler.this.writeValue(b -> {
                    return b.remove(toByteArray(valueIn));
                });

                return;
            }

            if (clear.contentEquals(eventName)) {
                writeVoid(BytesChronicleMap::clear);
                return;
            }

            if (replace.contentEquals(eventName)) {


                    // todo fix this this is a hack to get to work for now.
                    // todo may use something like :
                    // todo bytesMap.replace(reader, reader, timestamp, identifier());

                valueIn.marshallable(wire -> {

                    final Params[] params = replace.params;
                    final byte[] key = wire.read(params[0]).bytes();
                    final byte[] value = wire.read(params[1]).bytes();

                    writeValueFromBytes(b -> ((Map<byte[], byte[]>) b.delegate).replace(key, value));

                });


                return;
            }

            if (replaceWithOldAndNewValue.contentEquals(eventName)) {

                write(bytesMap -> {
                    final net.openhft.lang.io.Bytes reader = toReader(valueIn,
                            replaceWithOldAndNewValue.params);
                    boolean result = bytesMap.replace(reader, reader, reader);
                    outWire.write(Fields.reply).bool(result);
                });

                return;
            }

            if (putIfAbsent.contentEquals(eventName)) {

                valueIn.marshallable(wire -> {

                    final Params[] params = putIfAbsent.params;
                    final byte[] key = wire.read(params[0]).bytes();
                    final byte[] value = wire.read(params[1]).bytes();

                    writeValueFromBytes(b -> ((Map<byte[], byte[]>) b.delegate).putIfAbsent(key, value));

                });

                return;
            }

            if (removeWithValue.contentEquals(eventName)) {
                write(bytesMap -> {
                    final net.openhft.lang.io.Bytes reader = toReader(valueIn, removeWithValue.params);
                    // todo call   outWire.write(result)
                    // .bool(bytesMap.remove(reader, reader, timestamp, identifier()));
                    outWire.write(reply).bool(bytesMap.remove(reader, reader));
                });
                return;
            }


            if (getApplicationVersion.contentEquals(eventName)) {
                write(b -> outWire.write(reply).text(applicationVersion()));
                return;
            }

            if (persistedDataVersion.contentEquals(eventName)) {
                write(b -> outWire.write(reply).text(persistedDataVersion()));

                return;
            }

            if (hashCode.contentEquals(eventName)) {
                write(b -> outWire.write(reply).int32(b.hashCode()));
                return;
            }

            throw new IllegalStateException("unsupported event=" + eventName);

        } finally {


            //  if (len > 4)
            if (EventGroup.IS_DEBUG) {
                long len = bytes.position() - SIZE_OF_SIZE;
                if (len == 0) {
                    System.out.println("--------------------------------------------\n" +
                            "server writes:\n\n<EMPTY>");
                } else {

                    try {
                        System.out.println("--------------------------------------------\n" +
                                "server writes:\n\n" +

                                Wires.fromSizePrefixedBlobs(bytes, SIZE_OF_SIZE, len));
                        //Bytes.toDebugString(bytes, SIZE_OF_SIZE, len));
                    } catch (Exception e) {
                        System.out.println("--------------------------------------------\n" +
                                "server writes:\n\n" +
                                Bytes.toDebugString(bytes, SIZE_OF_SIZE, len));
                    }
                }
            }
        }


    }

    // todo remove
    private byte[] toByteArray(net.openhft.lang.io.Bytes bytes) {
        if (bytes == null || bytes.remaining() == 0)
            return new byte[]{};

        if (bytes.remaining() > Integer.MAX_VALUE)
            throw new BufferOverflowException();

        byte[] result = new byte[(int) bytes.remaining()];
        bytes.write(result);
        return result;
    }

    // todo remove
    private byte[] toBytes(WireKey fieldName) {

        final Wire wire = inWire;

        final ValueIn read = wire.read(fieldName);
        final long l = read.readLength();

        if (l > Integer.MAX_VALUE)
            throw new BufferOverflowException();

        final int fieldLength = (int) l;

        Bytes<?> bytes1 = wire.bytes();
        final long endPos = bytes1.position() + fieldLength;
        final long limit = bytes1.limit();

        try {
            byte[] bytes = new byte[fieldLength];

            bytes1.read(bytes);
            return bytes;
        } finally {
            bytes1.position(endPos);
            bytes1.limit(limit);
        }
    }

    private void putAll(long tid) {

        final BytesChronicleMap bytesMap = bytesMap(MapWireHandler.this.channelId);

        if (bytesMap == null)
            return;

        // note: a number of client threads can be using the same socket
        Runnable runnable = incompleteWork.get(tid);

        if (runnable != null) {
            runnable.run();
            return;
        }

        // Note : you can not assume that all the entries in a putAll will be continuous,
        // they maybe other transactions from other threads.
        // we it should be possible for a single entry to fill the Tcp buffer, so each entry
        // should have the ability to be processed separately
        // and then only applied to the map once all the entries are received.
        runnable = new Runnable() {

            // we should try and collect the data and then apply it atomically as quickly possible
            final Map<byte[], byte[]> collectData = new HashMap<>();

            @Override
            public void run() {


                // the old code assumed that ALL of the entries would fit into a single buffer
                // this assumption is invalid
                boolean hasNext;
                for (; ; ) {

                    hasNext = inWire.read(Fields.hasNext).bool();
                    // todo remove  toBytes()
                    collectData.put(toBytes(arg1), toBytes(arg2));

                    if (!hasNext) {

                        incompleteWork.remove(tid);

                        outWire.write(Fields.tid).int64(tid);

                        writeVoid(() -> {
                            bytesMap.delegate.putAll((Map) collectData);
                            return null;
                        });
                        return;
                    }
                    final Bytes<?> bytes = inWire.bytes();
                    if (bytes.remaining() == 0)
                        return;
                }
            }
        };

        incompleteWork.put(tid, runnable);
        runnable.run();

    }

    @SuppressWarnings("SameReturnValue")
    private void writeValueFromBytes(final Function<BytesChronicleMap, byte[]> f) {

        write(b -> {

            byte[] fromBytes = f.apply(b);
            boolean isNull = fromBytes == null || fromBytes.length == 0;
            outWire.write(reply);

            if (isNull)
                outWire.writeValue().text(null);
            else {
                Bytes<?> bytes = outWire.bytes();
                bytes.write(fromBytes);
            }

        });
    }

    private byte identifier() {
        // if the client provides a remote identifier then we will use that otherwise we will use
        // the local identifier
        return remoteIdentifier == 0 ? localIdentifier : remoteIdentifier;
    }

    @NotNull
    private CharSequence persistedDataVersion() {
        final BytesChronicleMap bytesChronicleMap = bytesMap(channelId);
        if (bytesChronicleMap == null)
            return "";
        return bytesChronicleMap.delegate.persistedDataVersion();
    }

    @NotNull
    private CharSequence applicationVersion() {
        return BuildVersion.version();
    }

    /**
     * creates a lang buffer that holds just the payload of the args
     *

     * @return a new lang buffer containing the bytes of the args
     */
    private net.openhft.lang.io.Bytes toReader(ValueIn valueIn, Params... params) {

        // todo this is a bit of a hack
        final Bytes<?> bytes1 = valueIn.wireIn().bytes();
        final long inSize = bytes1.remaining();

        final net.openhft.lang.io.Bytes bytes = DirectStore.allocate(inSize + 1024).bytes();


        valueIn.marshallable(new ReadMarshallable() {
            @Override
            public void readMarshallable(WireIn wire) throws IllegalStateException {
                for (Params p : params) {

                    final ValueIn read = wire.read(p);
                    final Bytes<?> bytes2 = read.wireIn().bytes();
                    final byte[] data = read.bytes();
                    System.out.println("out='" + new String(data) + "'");
                    bytes.writeStopBit(data.length);
                    bytes.write(data);
                }

            }
        });


        return bytes.flip();
    }

    /**
     * creates a lang buffer that holds just the payload of the args
     *
     * @param wire the inbound wire
     * @return a new lang buffer containing the bytes of the args
     */

    // todo remove this method - just added to get it to work for now
    private byte[] toByteArray(@NotNull Wire wire, @NotNull Params field) {

        final ValueIn read = wire.read(field);
        return toByteArray(read);
    }

    private byte[] toByteArray(ValueIn valueIn) {

        final String text = valueIn.text();
        return text.getBytes();



    }

    /**
     * gets the map for this channel id
     *
     * @param channelId the ID of the map
     * @return the chronicle map with this {@code channelId}
     */
    @NotNull
    private ReplicatedChronicleMap map(short channelId) {

        // todo this cast is a bit of a hack, improve later
        final ReplicatedChronicleMap map =
                (ReplicatedChronicleMap) channels.get(channelId);

        if (map != null)
            return map;

        throw new IllegalStateException();
    }

    /**
     * this is used to push the data straight into the entry in memory
     *
     * @param channelId the ID of the map
     * @return a BytesChronicleMap used to update the memory which holds the chronicle map
     */
    @Nullable
    private BytesChronicleMap bytesMap(short channelId) {

        final BytesChronicleMap bytesChronicleMap = (channelId < bytesChronicleMaps.size())
                ? bytesChronicleMaps.get(channelId)
                : null;

        if (bytesChronicleMap != null)
            return bytesChronicleMap;

        // grow the array
        for (int i = bytesChronicleMaps.size(); i <= channelId; i++) {
            bytesChronicleMaps.add(null);
        }

        final ReplicatedChronicleMap delegate = map(channelId);
        final BytesChronicleMap element = new BytesChronicleMap(delegate);
        bytesChronicleMaps.set(channelId, element);
        return element;

    }

    @SuppressWarnings("SameReturnValue")
    private void writeValue(final Function<Map<byte[], byte[]>, byte[]> f) {
        writeValueFromBytes(b -> {
            final Map<byte[], byte[]> delegate = (Map) b.delegate;
            return f.apply(delegate);
        });
    }

    @SuppressWarnings("SameReturnValue")
    private void writeValueUsingDelegate(final Function<ChronicleMap<byte[], byte[]>, byte[]> f) {

        write(b -> {

            byte[] result = f.apply((ChronicleMap<byte[], byte[]>) b.delegate);
            boolean isNull = result == null || result.length == 0;

            outWire.write(Fields.reply);
            final Bytes<?> bytes = outWire.bytes();

            if (isNull)
                outWire.writeValue().text(null);
            else
                bytes.write(result);

        });


    }

    @SuppressWarnings("SameReturnValue")
    private void write(@NotNull Consumer<BytesChronicleMap> c) {

        final BytesChronicleMap bytesMap = bytesMap(channelId);

        if (bytesMap == null) {
            LOG.error("no map for channelId=" + channelId + " can be found.");
            return;
        }

        bytesMap.output = null;
        //   final Bytes<?> bytes = outWire.bytes();
        //     bytes.mark();
        //   outWire.write(isException).bool(false);


        outWire.writeDocument(false, out -> {
            try {
                outWire.bytes().mark();
                c.accept(bytesMap);
            } catch (Exception e) {
                //      bytes.reset();
                // the idea of wire is that is platform independent,
                // so we wil have to send the exception as a String
                outWire.bytes().reset();

                WireOut o = out.write(reply)
                        .type(e.getClass().getSimpleName());

                if (e.getMessage() != null) {
                    o.writeValue().text(e.getMessage());
                }
                LOG.error("", e);
            }
        });


    }

    @SuppressWarnings("SameReturnValue")
    private void writeVoid(@NotNull Callable r) {

        final BytesChronicleMap bytesMap = bytesMap(channelId);

        if (bytesMap == null) {
            LOG.error("no map for channelId=" + channelId + " can be found.");
            return;
        }

        bytesMap.output = null;


        try {
            r.call();
        } catch (Exception e) {
            //      bytes.reset();
            // the idea of wire is that is platform independent,
            // so we wil have to send the exception as a String
            outWire.writeDocument(false, out -> {
                out.write(reply)
                        .type(e.getClass().getSimpleName())
                        .writeValue().text(e.getMessage());

            });

            LOG.error("", e);
        }


    }

    @SuppressWarnings("SameReturnValue")
    private void writeVoid(@NotNull Consumer<BytesChronicleMap> process) {

        // skip 4 bytes where we will write the size
        final BytesChronicleMap bytesMap = bytesMap(channelId);

        if (bytesMap == null) {
            LOG.error("no map for channelId=" + channelId + " can be found.");
            return;
        }



            try {
                outWire.bytes().mark();
                process.accept(bytesMap);

            } catch (Exception e) {
                outWire.writeDocument(false, out -> {
                //      bytes.reset();
                // the idea of wire is that is platform independent,
                // so we wil have to send the exception as a String
                outWire.bytes().reset();
                out.write(reply)
                        .type(e.getClass().getSimpleName())
                        .writeValue().text(e.getMessage());
                LOG.error("", e);
            });
        }


    }

    /**
     * only used for debugging
     */
    @SuppressWarnings("UnusedDeclaration")
    private void showOutWire() {
        Bytes<?> bytes = outWire.bytes();
        System.out.println("pos=" + bytes.position() + ",bytes=" +
                Bytes.toDebugString(bytes, 0, bytes.position()));
    }

    /**
     * converts the exception into a String, so that it can be sent to c# clients
     */
    private String toString(@NotNull Throwable t) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        return sw.toString();
    }

    public void localIdentifier(byte localIdentifier) {
        this.localIdentifier = localIdentifier;
    }

    static enum Params implements WireKey {

        key,
        value,
        oldValue,
        newValue
    }

    static enum EventId implements WireKey {

        longSize,
        size,
        isEmpty,
        containsKey(key),
        containsValue(value),
        get(key),
        put(key, value),
        putWithoutAcc(key, value),
        remove(key),
        removeWithoutAcc(key),
        clear,
        keySet,
        values,
        entrySet,
        entrySetRestricted,
        replace(key, value),
        replaceWithOldAndNewValue(key, oldValue, newValue),
        putIfAbsent(key, value),
        removeWithValue(key, value),
        toString,
        getApplicationVersion,
        persistedDataVersion,
        putAll,
        putAllWithoutAcc,
        hashCode,
        mapForKey,
        putMapped,
        keyBuilder,
        valueBuilder,
        createChannel,
        remoteIdentifier;

        private final Params[] params;

        EventId(Params... params) {
            this.params = params;
        }

        public boolean contentEquals(CharSequence c) {
            return this.toString().contentEquals(c);
        }

        public Params[] params() {
            return this.params;

        }
    }


}

