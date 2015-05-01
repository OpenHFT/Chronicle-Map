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
import net.openhft.chronicle.engine.client.ClientWiredStatelessTcpConnectionHub;
import net.openhft.chronicle.hash.impl.util.BuildVersion;
import net.openhft.chronicle.network.event.EventGroup;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static net.openhft.chronicle.map.MapWireHandlerProcessor.EventId.*;
import static net.openhft.chronicle.map.MapWireHandlerProcessor.Params.*;
import static net.openhft.chronicle.wire.CoreFields.reply;
import static net.openhft.chronicle.wire.Wires.acquireStringBuilder;


/**
 * @author Rob Austin.
 */
public class MapWireHandlerProcessor<K, V> implements
        MapWireHandler<ConcurrentMap<K, V>, K, V>,
        Consumer<WireHandlers> {

    private CharSequence csp;

    private BiConsumer<ValueOut, V> vToWire;
    private Function<ValueIn, K> wireToK;
    private Function<ValueIn, V> wireToV;

    @Override
    public void process(@NotNull final Wire in,
                        @NotNull final Wire out,
                        @NotNull final ConcurrentMap<K, V> map,
                        @NotNull final CharSequence csp,
                        @NotNull final BiConsumer<ValueOut, V> vToWire,
                        @NotNull final Function<ValueIn, K> kFromWire,
                        @NotNull final Function<ValueIn, V> vFromWire) throws StreamCorruptedException {


        this.vToWire = vToWire;
        this.wireToK = kFromWire;
        this.wireToV = vFromWire;

        try {
            this.inWire = in;
            this.outWire = out;
            this.map = map;
            this.csp = csp;
            inWire.readDocument(metaDataConsumer, dataConsumer);
        } catch (Exception e) {
            LOG.error("", e);
        }

    }


    enum Params implements WireKey {
        key,
        value,
        oldValue,
        newValue
    }

  public  enum EventId implements ParameterizeWireKey {
        longSize,
        size,
        isEmpty,
        containsKey(key),
        containsValue(value),
        get(key),
        getAndPut(key, value),
        put(key, value),
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

        private final WireKey[] params;

        <P extends WireKey> EventId(P... params) {
            this.params = params;
        }

        public <P extends WireKey> P[] params() {
            return (P[]) this.params;
        }
    }


    private static final Logger LOG = LoggerFactory.getLogger(MapWireHandlerProcessor.class);

    public static final int SIZE_OF_SIZE = ClientWiredStatelessTcpConnectionHub.SIZE_OF_SIZE;

    private final Map<Long, CharSequence> cidToCsp;
    @NotNull
    private final Map<CharSequence, Long> cspToCid = new HashMap<>();

    private Wire inWire = null;
    private Wire outWire = null;


    private final Consumer<WireIn> metaDataConsumer = new Consumer<WireIn>() {

        @Override
        public void accept(WireIn wireIn) {

            StringBuilder sb = Wires.acquireStringBuilder();

            for (; ; ) {
                final ValueIn read = inWire.read(sb);
                if (CoreFields.tid.contentEquals(sb)) {
                    tid = read.int64();
                    break;
                }
            }


        }
    };

    private ConcurrentMap<K, V> map;

    public MapWireHandlerProcessor(@NotNull final Map<Long, CharSequence> cidToCsp) throws IOException {
        this.cidToCsp = cidToCsp;
    }

    @Override
    public void accept(@NotNull final WireHandlers wireHandlers) {

    }

    public long tid;
    private final AtomicLong cid = new AtomicLong();

    /**
     * create a new cid if one does not already exist for this csp
     *
     * @param csp the csp we wish to check for a cid
     * @return the cid for this csp
     */
    private long createCid(CharSequence csp) {

        final long newCid = cid.incrementAndGet();
        final Long aLong = cspToCid.putIfAbsent(csp, newCid);

        if (aLong != null)
            return aLong;

        cidToCsp.put(newCid, csp.toString());
        return newCid;

    }

    final StringBuilder eventName = new StringBuilder();

    private final Consumer<WireIn> dataConsumer = new Consumer<WireIn>() {

        @Override
        public void accept(WireIn wireIn) {
            final Bytes<?> outBytes = outWire.bytes();
            try {

                final ValueIn valueIn = inWire.readEventName(eventName);

                outWire.writeDocument(true, wire -> outWire.write(CoreFields.tid).int64(tid));

                writeData(out -> {

                    if (clear.contentEquals(eventName)) {
                        map.clear();
                        return;
                    }

                    if (putAll.contentEquals(eventName)) {

                        final Map data = new HashMap();
                        while (valueIn.hasNext()) {
                            valueIn.sequence(v -> valueIn.marshallable(wire -> data.put(
                                    wireToK.apply(wire.read(put.params()[0])),
                                    wireToV.apply(wire.read(put.params()[1])))));
                        }

                        map.putAll(data);
                        return;
                    }


                    if (EventId.putIfAbsent.contentEquals(eventName)) {

                        valueIn.marshallable(wire -> {

                            final Params[] params = putIfAbsent.params();
                            final K key = wireToK.apply(wire.read(params[0]));
                            final V value = wireToV.apply(wire.read(params[1]));
                            final V v = map.putIfAbsent(key, value);
                            vToWire.accept(outWire.write(reply), v);

                        });

                        return;
                    }


                    // -- THESE METHODS ARE USED BOTH MY MAP AND ENTRY-SET
                    if (size.contentEquals(eventName)) {
                        outWire.write(reply).int32(map.size());
                        return;
                    }


                    if (longSize.contentEquals(eventName)) {
                        outWire.write(reply).int32(map.size());
                        return;
                    }


                    if (isEmpty.contentEquals(eventName)) {
                        outWire.write(reply).bool(map.isEmpty());
                        return;
                    }

                    if (keySet.contentEquals(eventName)) {
                        throw new UnsupportedOperationException("todo");
                    }

                    if (values.contentEquals(eventName)) {
                        throw new UnsupportedOperationException("todo");
                    }


                    if (entrySet.contentEquals(eventName)) {
                        outWire.write(reply).type("set-proxy").writeValue()

                                .marshallable(w -> {
                                    CharSequence root = csp.subSequence(0, csp
                                            .length() - "#map".length());

                                    final StringBuilder csp = acquireStringBuilder()
                                            .append(root)
                                            .append("#entrySet");

                                    w.write(CoreFields.csp).text(csp);
                                    w.write(CoreFields.cid).int64(createCid(csp));
                                });


                        return;
                    }

                    if (size.contentEquals(eventName)) {
                        outWire.write(reply).int64(map.size());
                        return;
                    }


                    if (containsKey.contentEquals(eventName)) {
                        outWire.write(reply)
                                .bool(map.containsKey(wireToK.apply(valueIn)));
                        return;
                    }

                    if (containsValue.contentEquals(eventName)) {
                        outWire.write(reply).bool(
                                map.containsValue(wireToV.apply(valueIn)));
                        return;
                    }

                    if (get.contentEquals(eventName)) {
                        final K key = wireToK.apply(valueIn);
                        vToWire.accept(outWire.write(reply),
                                map.get(key));
                        return;
                    }

                    if (getAndPut.contentEquals(eventName)) {

                        valueIn.marshallable(wire -> {

                            final Params[] params = getAndPut.params();
                            final K key = wireToK.apply(wire.read(params[0]));
                            final V value = wireToV.apply(wire.read(params[1]));

                            vToWire.accept(outWire.write(reply),
                                    map.put(key, value));

                        });

                        return;
                    }

                    if (remove.contentEquals(eventName)) {
                        outWire.write(reply);
                        final K key = wireToK.apply(valueIn);
                        vToWire.accept(outWire.write(reply), map.remove(key));
                        return;
                    }


                    if (replace.contentEquals(eventName)) {
                        valueIn.marshallable(wire -> {
                            final Params[] params = replace.params();
                            final K key = wireToK.apply(wire.read(params[0]));
                            final V value = wireToV.apply(wire.read(params[1]));

                            vToWire.accept(outWire.write(reply),
                                    map.replace(key, value));

                        });


                        return;
                    }

                    if (replaceWithOldAndNewValue.contentEquals(eventName)) {
                        valueIn.marshallable(wire -> {
                            final Params[] params = replaceWithOldAndNewValue.params();
                            final K key = wireToK.apply(wire.read(params[0]));
                            final V oldValue = wireToV.apply(wire.read(params[1]));
                            final V newValue = wireToV.apply(wire.read(params[2]));
                            outWire.write(reply).bool(map.replace(key, oldValue, newValue));

                        });
                        return;
                    }

                    if (putIfAbsent.contentEquals(eventName)) {
                        valueIn.marshallable(wire -> {
                            final Params[] params = putIfAbsent.params();
                            final K key = wireToK.apply(wire.read(params[0]));
                            final V value = wireToV.apply(wire.read(params[1]));
                            vToWire.accept(outWire.write(reply),
                                    map.putIfAbsent(key, value));

                        });

                        return;
                    }

                    if (removeWithValue.contentEquals(eventName)) {
                        throw new UnsupportedOperationException("todo");
                    }


                    if (getApplicationVersion.contentEquals(eventName)) {
                        outWire.write(reply).text(applicationVersion());
                        return;
                    }


                    if (hashCode.contentEquals(eventName)) {
                        outWire.write(reply).int32(map.hashCode());
                        return;
                    }

                    throw new IllegalStateException("unsupported event=" + eventName);
                });
            } catch (Exception e) {
                LOG.error("", e);
            } finally

            {

                if (EventGroup.IS_DEBUG) {

                    long len = outBytes.position() - SIZE_OF_SIZE;
                    if (len == 0) {
                        System.out.println("--------------------------------------------\n" +
                                "server writes:\n\n<EMPTY>");
                    } else {


                        System.out.println("--------------------------------------------\n" +
                                "server writes:\n\n" +
                                Wires.fromSizePrefixedBlobs(outBytes, SIZE_OF_SIZE, len));

                    }
                }
            }


        }
    };


    @NotNull
    private CharSequence applicationVersion() {
        return BuildVersion.version();
    }
    private long start;


    /**
     * write and exceptions and rolls back if no data was written
     */
    private void writeData(@NotNull Consumer<WireOut> c) {

        final long position = outWire.bytes().position();

        outWire.writeDocument(false, out -> {

            start = outWire.bytes().position();

            try {
                c.accept(outWire);

            } catch (Exception e) {

                LOG.error("", e);
                out.getValueOut()
                        .type(e.getClass().getSimpleName())
                        .writeValue().text(e.getMessage());
            }
        });

        // rollback if no data was written
        if (start == outWire.bytes().position()) {
            if (outWire.bytes().position() == start) {
                outWire.bytes().position(position);
            }

        }

    }


}

