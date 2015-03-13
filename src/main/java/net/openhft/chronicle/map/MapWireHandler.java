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
import net.openhft.chronicle.network2.WireHandler;
import net.openhft.chronicle.network2.event.EventGroup;
import net.openhft.chronicle.network2.event.WireHandlers;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireKey;
import net.openhft.lang.io.DirectStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StreamCorruptedException;
import java.io.StringWriter;
import java.nio.BufferOverflowException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static net.openhft.chronicle.map.MapWireHandlerBuilder.Fields.*;

/**
 * @author Rob Austin.
 */
class MapWireHandler<K, V> implements WireHandler, Consumer<WireHandlers> {

    private static final Logger LOG = LoggerFactory.getLogger(MapWireHandler.class);
    public static final int SIZE_OF_SIZE = 2;

    private final ArrayList<BytesChronicleMap> bytesChronicleMaps = new ArrayList<>();
    @NotNull

    private final StringBuilder methodName = new StringBuilder();
    private final Supplier<ChronicleHashInstanceBuilder<ChronicleMap<K, V>>> chronicleHashInstanceBuilder;
    private Wire inWire = null;
    private Wire outWire = null;
    private final Consumer writeElement = new Consumer<Iterator<byte[]>>() {

        @Override
        public void accept(Iterator<byte[]> iterator) {
            outWire.write(RESULT);
            outWire.bytes().write(iterator.next());
        }
    };

    private WireHandlers publishLater;

    @Override
    public void accept(WireHandlers wireHandlers) {
        this.publishLater = wireHandlers;
    }


    private final Consumer writeEntry = new Consumer<Iterator<Map.Entry<byte[], byte[]>>>() {

        @Override
        public void accept(Iterator<Map.Entry<byte[], byte[]>> iterator) {

            final Map.Entry<byte[], byte[]> entry = iterator.next();

            outWire.write(RESULT_KEY);
            outWire.bytes().write(entry.getKey());

            outWire.write(RESULT_VALUE);
            outWire.bytes().write(entry.getValue());
        }
    };
    private byte localIdentifier;
    private long timestamp;
    private short channelId;
    private List<Replica> channelList;
    private ReplicationHub hub;
    private byte remoteIdentifier;


    public MapWireHandler(@NotNull final Supplier<ChronicleHashInstanceBuilder<ChronicleMap<K, V>>> chronicleHashInstanceBuilder,
                          @NotNull final ReplicationHub hub,
                          byte localIdentifier,
                          @NotNull final List<Replica> channelList) {
        this(chronicleHashInstanceBuilder, hub);
        this.channelList = channelList;
        this.localIdentifier = localIdentifier;


    }

    public MapWireHandler(Supplier<ChronicleHashInstanceBuilder<ChronicleMap<K, V>>> chronicleHashInstanceBuilder, ReplicationHub hub) {
        this.chronicleHashInstanceBuilder = chronicleHashInstanceBuilder;
        this.hub = hub;
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


    private void writeChunked(long transactionId, @NotNull final Function<BytesChronicleMap, Iterator> function,
                              @NotNull final Consumer<Iterator> c) throws StreamCorruptedException {

        final BytesChronicleMap m = bytesMap(channelId);
        final Iterator<byte[]> iterator = function.apply(m);

        final WireHandler that = new WireHandler() {

            @Override
            public void process(Wire in, Wire out) throws StreamCorruptedException {

                outWire.write(TRANSACTION_ID).int64(transactionId);

                // this allows us to write more data than the buffer will allow
                for (; ; ) {

                    final boolean hasNext = iterator.hasNext();

                    write(map -> {

                        outWire.write(HAS_NEXT).bool(hasNext);

                        if (hasNext)
                            c.accept(iterator);

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

   /* private void writeEntryChunked(long transactionId,
                                   @NotNull final Function<BytesChronicleMap, Iterator> function,
                                   @NotNull final Consumer<Iterator<Map.Entry<byte[], byte[]>>> c) {

        final BytesChronicleMap m = bytesMap(channelId);
        final Iterator<Map.Entry<byte[], byte[]>> iterator = function.apply(m);

        // this allows us to write more data than the buffer will allow
        for (; ; ) {

            // each chunk has its own transaction-id
            outWire.write(TRANSACTION_ID).int64(transactionId);

            write(map -> {

                boolean hasNext = iterator.hasNext();
                outWire.write(HAS_NEXT).bool(hasNext);

                if (!hasNext)
                    return;

                c.accept(iterator);

            });

        }

    }*/

    @SuppressWarnings("UnusedReturnValue")
    void onEvent() throws StreamCorruptedException {

        // it is assumed by this point that the buffer has all the bytes in it for this message

        long transactionId = inWire.read(TRANSACTION_ID).int64();
        timestamp = inWire.read(TIME_STAMP).int64();
        channelId = inWire.read(CHANNEL_ID).int16();
        inWire.read(METHOD_NAME).text(methodName);

        if ("PUT_WITHOUT_ACC".contentEquals(methodName)) {
            writeVoid(bytesMap -> {
                final net.openhft.lang.io.Bytes reader = toReader(inWire, ARG_1, ARG_2);
                // todo  bytesMap.put(reader, reader, timestamp, identifier());
                bytesMap.put(reader, reader);
            });
            return;
        }

        if ("KEY_SET".contentEquals(methodName)) {
            writeChunked(transactionId, map -> map.keySet().iterator(), writeElement);
            return;
        }

        if ("VALUES".contentEquals(methodName)) {
            writeChunked(transactionId, map -> map.delegate.values().iterator(), writeElement);
            return;
        }

        if ("ENTRY_SET".contentEquals(methodName)) {
            writeChunked(transactionId, m -> m.delegate.entrySet().iterator(), writeEntry);
            return;
        }

        // write the transaction id
        outWire.write(() -> "TRANSACTION_ID").int64(transactionId);

        try {


            if ("PUT_ALL".contentEquals(methodName)) {
                putAll(transactionId);
                return;
            }

            if ("CREATE_CHANNEL".contentEquals(methodName)) {
                writeVoid(() -> {
                    short channelId1 = inWire.read(ARG_1).int16();
                    chronicleHashInstanceBuilder.get().replicatedViaChannel(hub.createChannel(channelId1)).create();
                    return null;
                });
                return;
            }


            if ("REMOTE_IDENTIFIER".contentEquals(methodName)) {
                this.remoteIdentifier = inWire.read(RESULT).int8();
                return;
            }

            if ("LONG_SIZE".contentEquals(methodName)) {
                write(b -> outWire.write(RESULT).int64(b.longSize()));
                return;
            }

            if ("IS_EMPTY".contentEquals(methodName)) {
                write(b -> outWire.write(RESULT).bool(b.isEmpty()));
                return;
            }

            if ("CONTAINS_KEY".contentEquals(methodName)) {
                write(b -> outWire.write(RESULT).bool(b.delegate.containsKey(toByteArray(inWire, ARG_1))));
                return;
            }

            if ("CONTAINS_VALUE".contentEquals(methodName)) {
                write(b -> outWire.write(RESULT).bool(b.delegate.containsValue(toByteArray(inWire, ARG_1))));
                return;
            }

            if ("GET".contentEquals(methodName)) {
                writeValueUsingDelegate(map -> map.get(toByteArray(inWire, ARG_1)));
                return;
            }

            if ("PUT".contentEquals(methodName)) {
                writeValue(b -> {
                    final net.openhft.lang.io.Bytes reader = MapWireHandler.this.toReader(inWire, ARG_1, ARG_2);

                    // todo call return b.put(reader, reader, timestamp, identifier());
                    return b.put(reader, reader);
                });

                return;
            }

            if ("REMOVE".contentEquals(methodName)) {
                writeValue(b -> b.remove(toReader(inWire, ARG_1)));
                return;
            }

            if ("CLEAR".contentEquals(methodName)) {
                writeVoid(BytesChronicleMap::clear);
                return;
            }

            if ("REPLACE".contentEquals(methodName)) {
                write(bytesMap -> {


                    // todo fix this this is a hack to get to work for now.
                    // todo may use somehting like :
                    // todo bytesMap.replace(reader, reader, timestamp, identifier());


                    VanillaChronicleMap map = bytesMap.delegate;
                    byte[] result = (byte[]) map.replace(
                            toByteArray(inWire, ARG_1),
                            toByteArray(inWire, ARG_2));

                    boolean isNull = result == null || result.length == 0;
                    outWire.write(() -> "RESULT_IS_NULL").bool(isNull);
                    if (!isNull) {
                        outWire.write(() -> "RESULT");
                        outWire.bytes().write(result);
                    }

                });
                return;
            }

            if ("REPLACE_WITH_OLD_AND_NEW_VALUE".contentEquals(methodName)) {

                write(bytesMap -> {
                    final net.openhft.lang.io.Bytes reader = toReader(inWire, ARG_1, ARG_2, ARG_3);
                    boolean result = bytesMap.replace(reader, reader, reader);
                    outWire.write(RESULT).bool(result);
                });

                return;
            }

            if ("PUT_IF_ABSENT".contentEquals(methodName)) {
                writeValue(bytesMap -> {
                    final net.openhft.lang.io.Bytes reader = toReader(inWire, ARG_1, ARG_2);
                    // todo call bytesMap.putIfAbsent(reader, reader, timestamp, identifier());
                    return bytesMap.putIfAbsent(reader, reader);
                });
                return;
            }

            if ("REMOVE_WITH_VALUE".contentEquals(methodName)) {
                write(bytesMap -> {
                    final net.openhft.lang.io.Bytes reader = toReader(inWire, ARG_1, ARG_2);
                    // todo call   outWire.write(RESULT).bool(bytesMap.remove(reader, reader, timestamp, identifier()));
                    outWire.write(RESULT).bool(bytesMap.remove(reader, reader));
                });
                return;
            }


            if ("APPLICATION_VERSION".contentEquals(methodName)) {
                write(b -> outWire.write(RESULT).text(applicationVersion()));
                return;
            }

            if ("PERSISTED_DATA_VERSION".contentEquals(methodName)) {
                write(b -> outWire.write(RESULT).text(persistedDataVersion()));

                return;
            }

            if ("HASH_CODE".contentEquals(methodName)) {
                write(b -> outWire.write(RESULT).int32(b.hashCode()));
                return;
            }

            throw new IllegalStateException("unsupported event=" + methodName);

        } finally {

            //  if (len > 4)
            if (EventGroup.IS_DEBUG) {
                long len = outWire.bytes().position() - SIZE_OF_SIZE;
                System.out.println("--------------------------------------------\nserver wrote:\n\n" + Bytes.toDebugString(outWire.bytes(), SIZE_OF_SIZE, len));
            }
        }


    }

    private byte[] toByteArray(net.openhft.lang.io.Bytes bytes) {
        if (bytes == null || bytes.remaining() == 0)
            return new byte[]{};

        if (bytes.remaining() > Integer.MAX_VALUE)
            throw new BufferOverflowException();

        byte[] result = new byte[(int) bytes.remaining()];
        bytes.write(result);
        return result;
    }

    private byte[] toBytes(WireKey fieldName) {

        final Wire wire = inWire;
        System.out.println(Bytes.toDebugString(inWire.bytes()));

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

        /*final BytesChronicleMap bytesMap = bytesMap(MapWireHandler.this.channelId);

        if (bytesMap == null)
            return;

        // note: a number of client threads can be using the same socket
        Runnable runnable = incompleteWork.get(transactionId);

        if (runnable != null) {
            runnable.run();
            return;
        }

        runnable = new Runnable() {

            // we should try and collect the data and then apply it atomically as quickly possible
            final Map<byte[], byte[]> collectData = new HashMap<>();

            @Override
            public void run() {
                if (inWire.read(HAS_NEXT).bool()) {
                    collectData.put(toBytes(ARG_1), toBytes(ARG_2));
                } else {
                    // the old code assumed that all the data would fit into a single buffer
                    // this assumption is invalid
                    if (!collectData.isEmpty()) {
                        bytesMap.delegate.putAll((Map) collectData);
                        incompleteWork.remove(transactionId);
                        outWire.write(TRANSACTION_ID).int64(transactionId);

                        // todo handle the case where there is an exception
                        outWire.write(IS_EXCEPTION).bool(false);

                    }
                }
            }
        };

        incompleteWork.put(transactionId, runnable);
        runnable.run();*/

    }

    private byte identifier() {
        // if the client provides a remote identifier then we will use that otherwise we will use the local identifier
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
        final ReplicatedChronicleMap replicas =
                (ReplicatedChronicleMap) channelList.get(channelId);

        if (replicas != null)
            return replicas;

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

        write(b -> {

            byte[] fromBytes = toByteArray(f.apply(b));
            boolean isNull = fromBytes.length == 0;
            outWire.write(RESULT_IS_NULL).bool(isNull);
            if (isNull)
                return;

            outWire.write(RESULT);
            outWire.bytes().write(fromBytes);

        });
    }


    @SuppressWarnings("SameReturnValue")
    private void writeValueUsingDelegate(final Function<ChronicleMap<byte[], byte[]>, byte[]> f) {

        write(b -> {

            byte[] result = f.apply((ChronicleMap) b.delegate);
            boolean isNull = result == null;

            outWire.write(RESULT_IS_NULL).bool(isNull);
            if (isNull)
                return;

            isNull = result.length == 0;

            outWire.write(RESULT);
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
        outWire.write(IS_EXCEPTION).bool(false);

        try {
            c.accept(bytesMap);
        } catch (Exception e) {
            outWire.bytes().reset();
            // the idea of wire is that is platform independent,
            // so we wil have to send the exception as a String
            outWire.write(IS_EXCEPTION).bool(true);
            outWire.write(EXCEPTION).text(toString(e));
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
        outWire.write(IS_EXCEPTION).bool(false);

        try {
            r.call();
        } catch (Exception e) {
            outWire.bytes().reset();
            // the idea of wire is that is platform independent,
            // so we wil have to send the exception as a String
            outWire.write(IS_EXCEPTION).bool(true);
            outWire.write(EXCEPTION).text(toString(e));
            LOG.error("", e);
            return;
        }

    }

    /**
     * only used for debugging
     */
    @SuppressWarnings("UnusedDeclaration")
    private void showOutWire() {
        System.out.println("pos=" + outWire.bytes().position() + ",bytes=" + Bytes.toDebugString(outWire.bytes(), 0, outWire.bytes().position()));
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
            outWire.write(IS_EXCEPTION).bool(false);
        } catch (Exception e) {
            outWire.write(IS_EXCEPTION).bool(true);
            LOG.error("", e);
        }

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


}
