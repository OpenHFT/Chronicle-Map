/*
 * Copyright 2014 Higher Frequency Trading
 * <p/>
 * http://www.higherfrequencytrading.com
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map;

import net.openhft.collections.*;
import net.openhft.lang.io.serialization.BytesMarshallerFactory;
import net.openhft.lang.io.serialization.ObjectSerializer;
import net.openhft.lang.model.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * @author Rob Austin.
 */
public class ChronicleMapBuilder<K, V> extends SharedHashMapBuilder<K, V> implements Cloneable {

    private static final Logger LOG = LoggerFactory.getLogger(ChronicleMapBuilder.class.getName());

    public ChronicleMapBuilder(Class<K> kClass, Class<V> vClass) {
        super(kClass, vClass);
    }

    public ChronicleMapBuilder() {
        super();
    }


    public static <K, V> ChronicleMapBuilder<K, V> of(Class<K> kClass, Class<V> vClass) {
        return new ChronicleMapBuilder<K, V>(kClass, vClass);
    }

    @Override
    public ChronicleMapBuilder<K, V> clone() {

        return (ChronicleMapBuilder) super.clone();
    }


    /**
     * Set minimum number of segments. See concurrencyLevel in {@link java.util.concurrent.ConcurrentHashMap}.
     *
     * @return this builder object back
     */
    public ChronicleMapBuilder minSegments(int minSegments) {
        super.minSegments(minSegments);
        return this;
    }


    /**
     * <p>Note that the actual entrySize will be aligned to 4 (default entry alignment). I. e. if you set
     * entry size to 30, the actual entry size will be 32 (30 aligned to 4 bytes). If you don't want entry
     * size to be aligned, set {@code entryAndValueAlignment(Alignment.NO_ALIGNMENT)}.
     *
     * @param entrySize the size in bytes
     * @return this {@code SharedHashMapBuilder} back
     * @see #entryAndValueAlignment(net.openhft.collections.Alignment)
     * @see #entryAndValueAlignment()
     */
    public ChronicleMapBuilder entrySize(int entrySize) {
        super.entrySize(entrySize);
        return this;
    }


    /**
     * Specifies alignment of address in memory of entries and independently of address in memory of values
     * within entries. <p/> <p>Useful when values of the map are updated intensively, particularly fields with
     * volatile access, because it doesn't work well if the value crosses cache lines. Also, on some (nowadays
     * rare) architectures any misaligned memory access is more expensive than aligned. <p/> <p>Note that
     * specified {@link #entrySize()} will be aligned according to this alignment. I. e. if you set {@code
     * entrySize(20)} and {@link net.openhft.collections.Alignment#OF_8_BYTES}, actual entry size will be 24
     * (20 aligned to 8 bytes).
     *
     * @return this {@code SharedHashMapBuilder} back
     * @see #entryAndValueAlignment()
     */
    public ChronicleMapBuilder<K, V> entryAndValueAlignment(Alignment alignment) {
        super.entryAndValueAlignment(alignment);
        return this;
    }


    public ChronicleMapBuilder<K, V> entries(long entries) {
        super.entries(entries);
        return this;
    }

    public ChronicleMapBuilder<K, V> replicas(int replicas) {
        super.replicas(replicas);
        return this;
    }


    public ChronicleMapBuilder<K, V> actualEntriesPerSegment(int actualEntriesPerSegment) {
        super.actualEntriesPerSegment(actualEntriesPerSegment);
        return this;
    }


    public ChronicleMapBuilder<K, V> actualSegments(int actualSegments) {
        super.actualSegments(actualSegments);
        return this;
    }


    /**
     * Not supported yet.
     *
     * @param transactional if the built map should be transactional
     * @return this {@code SharedHashMapBuilder} back
     */
    public ChronicleMapBuilder<K, V> transactional(boolean transactional) {
        super.transactional(transactional);
        return this;
    }


    public ChronicleMapBuilder kClass(Class kClass) {
        super.kClass(kClass);
        return this;
    }


    public SharedHashMapBuilder<K, V> file(File file) {
        super.file(file);
        return this;
    }


    public ChronicleMapBuilder lockTimeOutMS(long lockTimeOutMS) {
        super.lockTimeOutMS(lockTimeOutMS);
        return this;
    }


    public ChronicleMapBuilder errorListener(SharedMapErrorListener errorListener) {
        super.errorListener(errorListener);
        return this;
    }


    /**
     * Map.put() returns the previous value, functionality which is rarely used but fairly cheap for HashMap.
     * In the case, for an off heap collection, it has to create a new object (or return a recycled one)
     * Either way it's expensive for something you probably don't use.
     *
     * @param putReturnsNull false if you want SharedHashMap.put() to not return the object that was replaced
     *                       but instead return null
     * @return an instance of the map builder
     */
    public ChronicleMapBuilder putReturnsNull(boolean putReturnsNull) {
        super.putReturnsNull(putReturnsNull);
        return this;
    }


    /**
     * Map.remove()  returns the previous value, functionality which is rarely used but fairly cheap for
     * HashMap. In the case, for an off heap collection, it has to create a new object (or return a recycled
     * one) Either way it's expensive for something you probably don't use.
     *
     * @param removeReturnsNull false if you want SharedHashMap.remove() to not return the object that was
     *                          removed but instead return null
     * @return an instance of the map builder
     */
    public ChronicleMapBuilder removeReturnsNull(boolean removeReturnsNull) {
        super.removeReturnsNull(removeReturnsNull);
        return this;
    }


    public ChronicleMapBuilder generatedValueType(boolean generatedValueType) {
        super.generatedValueType(generatedValueType);
        return this;
    }


    public ChronicleMapBuilder largeSegments(boolean largeSegments) {
        super.largeSegments(largeSegments);
        return this;
    }


    public ChronicleMapBuilder metaDataBytes(int metaDataBytes) {
        super.metaDataBytes(metaDataBytes);
        return this;
    }


    public ChronicleMapBuilder canReplicate(boolean canReplicate) {
        super.canReplicate(canReplicate);
        return this;
    }


    public ChronicleMapBuilder timeProvider(TimeProvider timeProvider) {
        super.timeProvider(timeProvider);
        return this;
    }

    public ChronicleMapBuilder identifier(byte identifier) {
        super.identifier(identifier);
        return this;
    }

    public ChronicleMapBuilder tcpReplicatorBuilder(TcpReplicatorBuilder tcpReplicatorBuilder) {
        super.tcpReplicatorBuilder(tcpReplicatorBuilder);
        return this;
    }


    public ChronicleMapBuilder udpReplicatorBuilder(UdpReplicatorBuilder udpReplicatorBuilder) {
        super.udpReplicatorBuilder(udpReplicatorBuilder);
        return this;
    }


    public ChronicleMapBuilder bytesMarshallerFactory(BytesMarshallerFactory bytesMarshallerFactory) {
        super.bytesMarshallerFactory(bytesMarshallerFactory);
        return this;
    }


    public ChronicleMapBuilder objectSerializer(ObjectSerializer objectSerializer) {
        super.objectSerializer(objectSerializer);
        return this;
    }

    public ChronicleMapBuilder externalReplicatorBuilder(@NotNull ExternalReplicatorBuilder externalReplicatorBuilder) {
        super.externalReplicatorBuilder(externalReplicatorBuilder);
        return this;
    }


}
