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

package net.openhft.chronicle.set;

import net.openhft.chronicle.hash.ChronicleHashBuilder;
import net.openhft.chronicle.hash.ChronicleHashInstanceBuilder;
import net.openhft.chronicle.hash.replication.SingleChronicleHashReplication;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.hash.replication.TimeProvider;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.lang.io.serialization.BytesMarshaller;
import net.openhft.lang.io.serialization.BytesMarshallerFactory;
import net.openhft.lang.io.serialization.ObjectFactory;
import net.openhft.lang.io.serialization.ObjectSerializer;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * {@code ChronicleSetBuilder} manages the whole set of {@link ChronicleSet} configurations, could
 * be used as a classic builder and/or factory.
 *
 * <p>{@code ChronicleMapBuilder} is mutable, see a note in {@link
 * ChronicleHashBuilder} interface documentation.
 *
 * @param <K> element type of the sets, created by this builder
 * @see ChronicleSet
 * @see ChronicleMapBuilder
 */
public final class ChronicleSetBuilder<K>
        implements ChronicleHashBuilder<K, ChronicleSet<K>, ChronicleSetBuilder<K>> {

    private ChronicleMapBuilder<K, DummyValue> chronicleMapBuilder;

    ChronicleSetBuilder(Class<K> keyClass) {
        chronicleMapBuilder = ChronicleMapBuilder.of(keyClass, DummyValue.class)
                .valueMarshallers(DummyValueMarshaller.INSTANCE, DummyValueMarshaller.INSTANCE)
                .valueSizeMarshaller(DummyValueMarshaller.INSTANCE);
    }

    public static <K> ChronicleSetBuilder<K> of(Class<K> keyClass) {
        return new ChronicleSetBuilder<>(keyClass);
    }

    @Override
    public ChronicleSetBuilder<K> clone() {
        try {
            @SuppressWarnings("unchecked")
            final ChronicleSetBuilder<K> result = (ChronicleSetBuilder<K>) super.clone();
            result.chronicleMapBuilder = chronicleMapBuilder.clone();
            return result;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public ChronicleSetBuilder<K> actualSegments(int actualSegments) {
        chronicleMapBuilder.actualSegments(actualSegments);
        return this;
    }

    @Override
    public ChronicleSetBuilder<K> minSegments(int minSegments) {
        chronicleMapBuilder.minSegments(minSegments);
        return this;
    }

    @Override
    public ChronicleSetBuilder<K> entriesPerSegment(long entriesPerSegment) {
        chronicleMapBuilder.entriesPerSegment(entriesPerSegment);
        return this;
    }

    @Override
    public ChronicleSetBuilder<K> actualChunksPerSegment(long actualChunksPerSegment) {
        chronicleMapBuilder.actualChunksPerSegment(actualChunksPerSegment);
        return this;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Example: if keys in your set(s) are English words in {@link String} form, average English
     * word length is 5.1, configure average key size of 6: <pre>{@code
     * ChronicleSet<String> uniqueWords = ChronicleSetBuilder.of(String.class)
     *     .entries(50000)
     *     .averageKeySize(6)
     *     .create();}</pre>
     *
     * <p>(Note that 6 is chosen as average key size in bytes despite strings in Java are UTF-16
     * encoded (and each character takes 2 bytes on-heap), because default off-heap {@link String}
     * encoding is UTF-8 in {@code ChronicleSet}.)
     *
     * @see #constantKeySizeBySample(Object)
     * @see #actualChunkSize(int)
     * @param averageKeySize
     */
    @Override
    public ChronicleSetBuilder<K> averageKeySize(double averageKeySize) {
        chronicleMapBuilder.averageKeySize(averageKeySize);
        return this;
    }

    /**
     * {@inheritDoc}
     *
     * <p>For example, if your keys are Git commit hashes:<pre>{@code
     * Set<byte[]> gitCommitsOfInterest = ChronicleSetBuilder.of(byte[].class)
     *     .constantKeySizeBySample(new byte[20])
     *     .create();}</pre>
     *
     * @see ChronicleHashBuilder#averageKeySize(double)
     */
    @Override
    public ChronicleSetBuilder<K> constantKeySizeBySample(K sampleKey) {
        chronicleMapBuilder.constantKeySizeBySample(sampleKey);
        return this;
    }

    @Override
    public ChronicleSetBuilder<K> actualChunkSize(int actualChunkSize) {
        chronicleMapBuilder.actualChunkSize(actualChunkSize);
        return this;
    }

    @Override
    public ChronicleSetBuilder<K> maxChunksPerEntry(int maxChunksPerEntry) {
        chronicleMapBuilder.maxChunksPerEntry(maxChunksPerEntry);
        return this;
    }

    @Override
    public ChronicleSetBuilder<K> entries(long entries) {
        chronicleMapBuilder.entries(entries);
        return this;
    }

    @Override
    public String toString() {
        return " ChronicleSetBuilder{" +
                "chronicleMapBuilder=" + chronicleMapBuilder +
                '}';
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(Object o) {
        return o instanceof ChronicleSetBuilder &&
                chronicleMapBuilder.equals(((ChronicleSetBuilder) o).chronicleMapBuilder);
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public ChronicleSetBuilder<K> timeProvider(TimeProvider timeProvider) {
        chronicleMapBuilder.timeProvider(timeProvider);
        return this;
    }

    @Override
    public ChronicleSetBuilder<K> bytesMarshallerFactory(
            BytesMarshallerFactory bytesMarshallerFactory) {
        chronicleMapBuilder.bytesMarshallerFactory(bytesMarshallerFactory);
        return this;
    }

    /**
     * {@inheritDoc}
     *
     * <p> Example: <pre>{@code Set<Key> set = ChronicleSetBuilder.of(Key.class)
     *     .entries(1_000_000)
     *     .keySize(100)
     *     // this class hasn't implemented yet, just for example
     *     .objectSerializer(new KryoObjectSerializer())
     *     .create();}</pre>
     */
    @Override
    public ChronicleSetBuilder<K> objectSerializer(ObjectSerializer objectSerializer) {
        chronicleMapBuilder.objectSerializer(objectSerializer);
        return this;
    }

    @Override
    public ChronicleSetBuilder<K> keyMarshaller(@NotNull BytesMarshaller<? super K> keyMarshaller) {
        chronicleMapBuilder.keyMarshaller(keyMarshaller);
        return this;
    }

    @Override
    public ChronicleSetBuilder<K> keyMarshallers(
            @NotNull BytesWriter<? super K> keyWriter, @NotNull BytesReader<K> keyReader) {
        chronicleMapBuilder.keyMarshallers(keyWriter, keyReader);
        return this;
    }

    @Override
    public ChronicleSetBuilder<K> keySizeMarshaller(@NotNull SizeMarshaller keySizeMarshaller) {
        chronicleMapBuilder.keySizeMarshaller(keySizeMarshaller);
        return this;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Actually this is just a convenience method supporting key marshaller configurations, made
     * initially during {@link #of(Class)} call. Because if you {@linkplain
     * #keyMarshaller(BytesMarshaller) configure} own custom key marshaller, this method doesn't
     * take any effect on the maps constructed by this builder.
     *
     * @see #of(Class)
     */
    @Override
    public ChronicleSetBuilder<K> keyDeserializationFactory(
            @NotNull ObjectFactory<? extends K> keyDeserializationFactory) {
        chronicleMapBuilder.keyDeserializationFactory(keyDeserializationFactory);
        return this;
    }

    @Override
    public ChronicleSetBuilder<K> immutableKeys() {
        chronicleMapBuilder.immutableKeys();
        return this;
    }

    @Override
    public ChronicleSetBuilder<K> replication(SingleChronicleHashReplication replication) {
        chronicleMapBuilder.replication(replication);
        return this;
    }

    @Override
    public ChronicleSetBuilder<K> replication(
            byte identifier, TcpTransportAndNetworkConfig tcpTransportAndNetwork) {
        chronicleMapBuilder.replication(identifier, tcpTransportAndNetwork);
        return this;
    }

    @Override
    public ChronicleSetBuilder<K> replication(byte identifier) {
        chronicleMapBuilder.replication(identifier);
        return this;
    }

    @Override
    public ChronicleHashInstanceBuilder<ChronicleSet<K>> instance() {
        return new SetInstanceBuilder<>(chronicleMapBuilder.instance());
    }

    @Override
    public ChronicleSet<K> create() {
        final ChronicleMap<K, DummyValue> map = chronicleMapBuilder.create();
        return new SetFromMap<>(map);
    }

    @Override
    public ChronicleSet<K> createPersistedTo(File file) throws IOException {
        ChronicleMap<K, DummyValue> map = chronicleMapBuilder.createPersistedTo(file);
        return new SetFromMap<>(map);
    }
}

