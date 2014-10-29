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

import net.openhft.chronicle.hash.*;
import net.openhft.chronicle.map.Alignment;
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
 * <p>{@code ChronicleMapBuilder} is mutable, see a note in {@link ChronicleHashBuilder} interface
 * documentation.
 *
 * @param <E> element type of the sets, created by this builder
 * @see ChronicleSet
 * @see ChronicleMapBuilder
 */
public class ChronicleSetBuilder<E>
        implements ChronicleHashBuilder<E, ChronicleSet<E>, ChronicleSetBuilder<E>> {

    private ChronicleMapBuilder<E, DummyValue> chronicleMapBuilder;

    ChronicleSetBuilder(Class<E> keyClass) {
        chronicleMapBuilder = ChronicleMapBuilder.of(keyClass, DummyValue.class)
                .valueMarshaller(DummyValueMarshaller.INSTANCE);
    }

    public static <K> ChronicleSetBuilder<K> of(Class<K> keyClass) {
        return new ChronicleSetBuilder<K>(keyClass);
    }

    @Override
    public ChronicleSetBuilder<E> clone() {
        try {
            @SuppressWarnings("unchecked")
            final ChronicleSetBuilder<E> result = (ChronicleSetBuilder<E>) super.clone();
            result.chronicleMapBuilder = chronicleMapBuilder.clone();
            return result;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public ChronicleSetBuilder<E> actualSegments(int actualSegments) {
        chronicleMapBuilder.actualSegments(actualSegments);
        return this;
    }

    @Override
    public ChronicleSetBuilder<E> minSegments(int minSegments) {
        chronicleMapBuilder.minSegments(minSegments);
        return this;
    }

    @Override
    public ChronicleSetBuilder<E> actualEntriesPerSegment(long actualEntriesPerSegment) {
        chronicleMapBuilder.actualEntriesPerSegment(actualEntriesPerSegment);
        return this;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Example: if keys in your set(s) are English words in {@link String} form, keys size 10
     * (a bit more than average English word length) would be a good choice: <pre>{@code
     * ChronicleSet<String> uniqueWords = ChronicleSetBuilder.of(String.class)
     *     .entries(50000)
     *     .keySize(10)
     *     .create();}</pre>
     *
     * <p>(Note that 10 is chosen as key size in bytes despite strings in Java are UTF-16 encoded
     * (and each character takes 2 bytes on-heap), because default off-heap {@link String} encoding
     * is UTF-8 in {@code ChronicleSet}.)
     *
     * @see #constantKeySizeBySample(Object)
     * @see #entrySize(int)
     */
    @Override
    public ChronicleSetBuilder<E> keySize(int keySize) {
        chronicleMapBuilder.keySize(keySize);
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
     * @see #keySize(int)
     */
    @Override
    public ChronicleSetBuilder<E> constantKeySizeBySample(E sampleKey) {
        chronicleMapBuilder.constantKeySizeBySample(sampleKey);
        return this;
    }

    /**
     * {@inheritDoc}
     *
     * <p>In fully default case you can expect entry size to be about 120-130 bytes. But it is
     * strongly recommended always to configure {@linkplain #keySize(int) key size}, if they
     * couldn't be derived statically.
     *
     * <p>If entry size is not configured explicitly by calling this method, it is computed based on
     * {@linkplain #metaDataBytes(int) meta data bytes}, plus {@linkplain #keySize(int) key size},
     * plus a few bytes required by implementations.
     */
    @Override
    public ChronicleSetBuilder<E> entrySize(int entrySize) {
        chronicleMapBuilder.entrySize(entrySize);
        return this;
    }

    @Override
    public ChronicleSetBuilder<E> entries(long entries) {
        chronicleMapBuilder.entries(entries);
        return this;
    }

    @Override
    public ChronicleSetBuilder<E> lockTimeOut(long lockTimeOut, TimeUnit unit) {
        chronicleMapBuilder.lockTimeOut(lockTimeOut, unit);
        return this;
    }

    @Override
    public ChronicleSetBuilder<E> errorListener(ChronicleHashErrorListener errorListener) {
        chronicleMapBuilder.errorListener(errorListener);
        return this;
    }

    @Override
    public ChronicleSetBuilder<E> metaDataBytes(int metaDataBytes) {
        chronicleMapBuilder.metaDataBytes(metaDataBytes);
        return this;
    }

    @Override
    public ChronicleSetBuilder<E> replicators(byte identifier, ReplicationConfig... replicators) {
        chronicleMapBuilder.replicators(identifier, replicators);
        return this;
    }

    @Override
    public ChronicleSetBuilder<E> channel(ChannelProvider.ChronicleChannel chronicleChannel) {
        chronicleMapBuilder.channel(chronicleChannel);
        return this;
    }

    @Override
    public ChronicleSetBuilder<E> disableReplication() {
        chronicleMapBuilder.disableReplication();
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
        if (!(o instanceof ChronicleSetBuilder))
            return false;
        return chronicleMapBuilder.equals(((ChronicleSetBuilder) o).chronicleMapBuilder);
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public ChronicleSetBuilder<E> timeProvider(TimeProvider timeProvider) {
        chronicleMapBuilder.timeProvider(timeProvider);
        return this;
    }

    @Override
    public ChronicleSetBuilder<E> bytesMarshallerFactory(
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
    public ChronicleSetBuilder<E> objectSerializer(ObjectSerializer objectSerializer) {
        chronicleMapBuilder.objectSerializer(objectSerializer);
        return this;
    }

    @Override
    public ChronicleSetBuilder<E> keyMarshaller(@NotNull BytesMarshaller<E> keyMarshaller) {
        chronicleMapBuilder.keyMarshaller(keyMarshaller);
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
    public ChronicleSetBuilder<E> keyDeserializationFactory(
            @NotNull ObjectFactory<E> keyDeserializationFactory) {
        chronicleMapBuilder.keyDeserializationFactory(keyDeserializationFactory);
        return this;
    }

    @Override
    public ChronicleSetBuilder<E> immutableKeys() {
        chronicleMapBuilder.immutableKeys();
        return this;
    }

    @Override
    public ChronicleSetBuilder<E> stateless(StatelessBuilder statelessBuilder) {
        chronicleMapBuilder.stateless(statelessBuilder);
        return this;
    }

    @Override
    public ChronicleSetBuilder<E> file(File file) throws IOException {
        chronicleMapBuilder.file(file);
        return this;
    }

    @Override
    public ChronicleSet<E> create() throws IOException {
        final ChronicleMap<E, DummyValue> map = chronicleMapBuilder.create();
        return new SetFromMap<E>(map);
    }



}

