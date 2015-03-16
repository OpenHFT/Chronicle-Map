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
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireKey;
import net.openhft.chronicle.wire.Wires;
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
            outWire.write(result);
            outWire.bytes().write(iterator.next());
        }
    };
    private final Consumer writeEntry = new Consumer<Iterator<Map.Entry<byte[], byte[]>>>() {

        @Override
        public void accept(Iterator<Map.Entry<byte[], byte[]>> iterator) {

            final Map.Entry<byte[], byte[]> entry = iterator.next();

            outWire.write(resultKey);
            outWire.bytes().write(entry.getKey());

            outWire.write(resultValue);
            outWire.bytes().write(entry.getValue());
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
     * @param transactionId the transaction id
     * @param function      provides that returns items bases on a BytesChronicleMap
     * @param c             an iterator that contains items
     * @param maxEntries    the maximum number of items that can be written
     * @throws StreamCorruptedException
     */
    private void writeChunked(
            long transactionId, @NotNull final Function<BytesChronicleMap, Iterator> function,
            @NotNull final Consumer<Iterator> c, long maxEntries) throws StreamCorruptedException {

        final BytesChronicleMap m = bytesMap(channelId);
        final Iterator iterator = function.apply(m);

        final WireHandler that = new WireHandler() {

            @Override
            public void process(Wire in, Wire out) throws StreamCorruptedException {

                outWire.write(Fields.transactionId).int64(transactionId);

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
                    if (outWire.bytes().remaining() < (outWire.bytes().capacity() * 0.75)) {
                        publishLater.add(this);
                        return;
                    }


                }
            }
        };

        that.process(inWire, outWire);

    }

    private void writeChunked(
            long transactionId, @NotNull final Function<BytesChronicleMap, Iterator> function,
            @NotNull final Consumer<Iterator> c) throws StreamCorruptedException {
        writeChunked(transactionId, function, c, Long.MAX_VALUE);
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

        long transactionId = inWire.read(Fields.transactionId).int64();
        timestamp = inWire.read(timeStamp).int64();
        channelId = inWire.read(Fields.channelId).int16();

        final StringBuilder methodName = Wires.acquireStringBuilder();
        inWire.read(Fields.methodName).text(methodName);

        if (!incompleteWork.isEmpty()) {
            Runnable runnable = incompleteWork.get(transactionId);
            if (runnable != null) {
                runnable.run();
                return;
            }
        }
        try {

            if (putWithoutAcc.contentEquals(methodName)) {
                writeVoid(bytesMap -> {
                    final net.openhft.lang.io.Bytes reader = toReader(inWire, arg1, arg2);
                    // todo  bytesMap.put(reader, reader, timestamp, identifier());
                    bytesMap.put(reader, reader);
                });
                return;
            }

            if (keySet.contentEquals(methodName)) {
                writeChunked(transactionId, map -> map.keySet().iterator(), writeElement);
                return;
            }

            if (values.contentEquals(methodName)) {
                writeChunked(transactionId, map -> map.delegate.values().iterator(), writeElement);
                return;
            }

            if (entrySet.contentEquals(methodName)) {
                writeChunked(transactionId, m -> m.delegate.entrySet().iterator(), writeEntry);
                return;
            }

            if (entrySetRestricted.contentEquals(methodName)) {
                long maxEntries = inWire.read(arg1).int64();
                writeChunked(transactionId, m -> m.delegate.entrySet().iterator(), writeEntry,maxEntries);
                return;
            }

            if (putAll.contentEquals(methodName)) {
                putAll(transactionId);
                return;
            }

            // write the transaction id
            outWire.write(Fields.transactionId).int64(transactionId);


            if (createChannel.contentEquals(methodName)) {
                writeVoid(() -> {
                    short channelId1 = inWire.read(arg1).int16();
                    mapFactory.get().replicatedViaChannel(hub.createChannel(channelId1)).create();
                    return null;
                });
                return;
            }

            if (EventId.remoteIdentifier.contentEquals(methodName)) {
                this.remoteIdentifier = inWire.read(result).int8();
                return;
            }

            if (longSize.contentEquals(methodName)) {
                write(b -> outWire.write(result).int64(b.longSize()));
                return;
            }

            if (isEmpty.contentEquals(methodName)) {
                write(b -> outWire.write(result).bool(b.isEmpty()));
                return;
            }

            if (containsKey.contentEquals(methodName)) {
                // todo remove the    toByteArray(..)
                write(b -> outWire.write(result)
                        .bool(b.delegate.containsKey(toByteArray(inWire, arg1))));
                return;
            }

            if (containsValue.contentEquals(methodName)) {
                // todo remove the    toByteArray(..)
                write(b -> outWire.write(result)
                        .bool(b.delegate.containsValue(toByteArray(inWire, arg1))));
                return;
            }

            if (get.contentEquals(methodName)) {
                // todo remove the    toByteArray(..)
                writeValueUsingDelegate(map -> map.get(toByteArray(inWire, arg1)));
                return;
            }

            if (put.contentEquals(methodName)) {
                writeValue(b -> {
                    final net.openhft.lang.io.Bytes reader =
                            MapWireHandler.this.toReader(inWire, arg1, arg2);

                    // todo call return b.put(reader, reader, timestamp, identifier());
                    return b.put(reader, reader);
                });

                return;
            }

            if (remove.contentEquals(methodName)) {
                writeValue(b -> b.remove(toReader(inWire, arg1)));
                return;
            }

            if (clear.contentEquals(methodName)) {
                writeVoid(BytesChronicleMap::clear);
                return;
            }

            if (replace.contentEquals(methodName)) {
                write(bytesMap -> {


                    // todo fix this this is a hack to get to work for now.
                    // todo may use something like :
                    // todo bytesMap.replace(reader, reader, timestamp, identifier());

                    final VanillaChronicleMap map = bytesMap.delegate;
                    byte[] result = (byte[]) map.replace(
                            toByteArray(inWire, arg1),
                            toByteArray(inWire, arg2));

                    boolean isNull = result == null || result.length == 0;
                    outWire.write(resultIsNull).bool(isNull);
                    if (!isNull) {
                        outWire.write(Fields.result);
                        outWire.bytes().write(result);
                    }

                });
                return;
            }

            if (replaceWithOldAndNewValue.contentEquals(methodName)) {

                write(bytesMap -> {
                    final net.openhft.lang.io.Bytes reader = toReader(inWire, arg1, arg2, arg3);
                    boolean result = bytesMap.replace(reader, reader, reader);
                    outWire.write(Fields.result).bool(result);
                });

                return;
            }

            if (putIfAbsent.contentEquals(methodName)) {


                // todo call bytesMap.putIfAbsent(reader, reader, timestamp, identifier());
                writeValueFromBytes(b -> {
                    // todo remove the    toByteArray(..)
                    byte[] key = MapWireHandler.this.toByteArray(inWire, arg1);
                    byte[] value = MapWireHandler.this.toByteArray(inWire, arg2);
                    return ((Map<byte[], byte[]>) b.delegate).putIfAbsent(key, value);
                });


                return;
            }

            if (removeWithValue.contentEquals(methodName)) {
                write(bytesMap -> {
                    final net.openhft.lang.io.Bytes reader = toReader(inWire, arg1, arg2);
                    // todo call   outWire.write(result)
                    // .bool(bytesMap.remove(reader, reader, timestamp, identifier()));
                    outWire.write(result).bool(bytesMap.remove(reader, reader));
                });
                return;
            }


            if (applicationVersion.contentEquals(methodName)) {
                write(b -> outWire.write(result).text(applicationVersion()));
                return;
            }

            if (persistedDataVersion.contentEquals(methodName)) {
                write(b -> outWire.write(result).text(persistedDataVersion()));

                return;
            }

            if (hashCode.contentEquals(methodName)) {
                write(b -> outWire.write(result).int32(b.hashCode()));
                return;
            }

            throw new IllegalStateException("unsupported event=" + methodName);

        } finally {

            //  if (len > 4)
            if (EventGroup.IS_DEBUG) {
                long len = outWire.bytes().position() - SIZE_OF_SIZE;
                if (len == 0) {
                    System.out.println("--------------------------------------------\n" +
                            "server writes:\n\n<EMPTY>");
                } else {
                    System.out.println("--------------------------------------------\n" +
                            "server writes:\n\n" +
                            Bytes.toDebugString(outWire.bytes(), SIZE_OF_SIZE, len));
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

        final long endPos = wire.bytes().position() + fieldLength;
        final long limit = wire.bytes().limit();

        try {
            byte[] bytes = new byte[fieldLength];

            wire.bytes().read(bytes);
            return bytes;
        } finally {
            wire.bytes().position(endPos);
            wire.bytes().limit(limit);
        }
    }

    private void putAll(long transactionId) {

        final BytesChronicleMap bytesMap = bytesMap(MapWireHandler.this.channelId);

        if (bytesMap == null)
            return;

        // note: a number of client threads can be using the same socket
        Runnable runnable = incompleteWork.get(transactionId);

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

                        incompleteWork.remove(transactionId);

                        outWire.write(Fields.transactionId).int64(transactionId);

                        writeVoid(() -> {
                            bytesMap.delegate.putAll((Map) collectData);
                            return null;
                        });
                        return;
                    }
                    if (inWire.bytes().remaining() == 0)
                        return;
                }
            }
        };

        incompleteWork.put(transactionId, runnable);
        runnable.run();

    }

    @SuppressWarnings("SameReturnValue")
    private void writeValueFromBytes(final Function<BytesChronicleMap, byte[]> f) {

        write(b -> {

            byte[] fromBytes = f.apply(b);
            boolean isNull = fromBytes == null || fromBytes.length == 0;
            outWire.write(resultIsNull).bool(isNull);
            if (isNull)
                return;

            outWire.write(result);
            outWire.bytes().write(fromBytes);

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
     * @param wire the inbound wire
     * @param args the key names of the {@code wire} args
     * @return a new lang buffer containing the bytes of the args
     */
    private net.openhft.lang.io.Bytes toReader(@NotNull Wire wire, @NotNull WireKey... args) {

        final long inSize = wire.bytes().limit();
        final net.openhft.lang.io.Bytes bytes = DirectStore.allocate(inSize).bytes();

        // copy the bytes to the reader
        for (final WireKey field : args) {

            final ValueIn read = wire.read(field);
            final long fieldLength = read.readLength();

            final long endPos = wire.bytes().position() + fieldLength;
            final long limit = wire.bytes().limit();

            try {

                final Bytes source = wire.bytes();
                source.limit(endPos);

                // write the size
                bytes.writeStopBit(source.remaining());

                while (source.remaining() > 0) {
                    if (source.remaining() >= 8)
                        bytes.writeLong(source.readLong());
                    else
                        bytes.writeByte(source.readByte());
                }

            } finally {
                wire.bytes().position(endPos);
                wire.bytes().limit(limit);
            }

        }

        return bytes.flip();
    }

    /**
     * creates a lang buffer that holds just the payload of the args
     *
     * @param wire the inbound wire
     * @return a new lang buffer containing the bytes of the args
     */

    // todo remove this method - just added to get it to work for now
    private byte[] toByteArray(@NotNull Wire wire, @NotNull WireKey field) {

        final ValueIn read = wire.read(field);
        final long fieldLength = read.readLength();

        final long endPos = wire.bytes().position() + fieldLength;
        final long limit = wire.bytes().limit();
        byte[] result = new byte[]{};
        try {

            final Bytes source = wire.bytes();
            source.limit(endPos);

            if (source.remaining() > Integer.MAX_VALUE)
                throw new BufferOverflowException();

            // write the size
            result = new byte[(int) source.remaining()];
            source.read(result);


        } finally {
            wire.bytes().position(endPos);
            wire.bytes().limit(limit);
        }


        return result;
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
    private void writeValue(final Function<BytesChronicleMap, net.openhft.lang.io.Bytes> f) {
        writeValueFromBytes(b -> toByteArray(f.apply(b)));
    }

    @SuppressWarnings("SameReturnValue")
    private void writeValueUsingDelegate(final Function<ChronicleMap<byte[], byte[]>, byte[]> f) {

        write(b -> {

            byte[] result = f.apply((ChronicleMap) b.delegate);
            boolean isNull = result == null;

            outWire.write(resultIsNull).bool(isNull);
            if (isNull)
                return;

            isNull = result.length == 0;

            outWire.write(Fields.result);
            outWire.bytes().write(result);

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
        outWire.bytes().mark();
        outWire.write(isException).bool(false);

        try {
            c.accept(bytesMap);
        } catch (Exception e) {
            outWire.bytes().reset();
            // the idea of wire is that is platform independent,
            // so we wil have to send the exception as a String
            outWire.write(isException).bool(true);
            outWire.write(exception).text(toString(e));
            LOG.error("", e);
            return;
        }

    }

    @SuppressWarnings("SameReturnValue")
    private void writeVoid(@NotNull Callable r) {

        final BytesChronicleMap bytesMap = bytesMap(channelId);

        if (bytesMap == null) {
            LOG.error("no map for channelId=" + channelId + " can be found.");
            return;
        }

        bytesMap.output = null;
        outWire.bytes().mark();

        try {
            r.call();
            outWire.write(isException).bool(false);
        } catch (Exception e) {
            outWire.bytes().reset();
            // the idea of wire is that is platform independent,
            // so we wil have to send the exception as a String
            outWire.write(isException).bool(true);
            outWire.write(exception).text(toString(e));
            LOG.error("", e);
            return;
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

        bytesMap.output = null;
        try {
            process.accept(bytesMap);
            outWire.write(isException).bool(false);
        } catch (Exception e) {
            outWire.write(isException).bool(true);
            LOG.error("", e);
        }

    }

    /**
     * only used for debugging
     */
    @SuppressWarnings("UnusedDeclaration")
    private void showOutWire() {
        System.out.println("pos=" + outWire.bytes().position() + ",bytes=" +
                Bytes.toDebugString(outWire.bytes(), 0, outWire.bytes().position()));
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

    static enum EventId {

        longSize,
        size,
        isEmpty,
        containsKey,
        containsValue,
        get,
        put,
        putWithoutAcc,
        remove,
        removeWithoutAcc,
        clear,
        keySet,
        values,
        entrySet,
        entrySetRestricted,
        replace,
        replaceWithOldAndNewValue,
        putIfAbsent,
        removeWithValue,
        toString,
        applicationVersion,
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

         public boolean contentEquals(CharSequence c) {
            return this.toString().contentEquals(c);
        }
    }


}
