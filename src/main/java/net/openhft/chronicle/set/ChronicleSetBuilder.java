/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.set;

import net.openhft.chronicle.hash.ChronicleHashBuilder;
import net.openhft.chronicle.hash.ChronicleHashErrorListener;
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
 * @param <E> element type of the sets, created by this builder
 * @see ChronicleSet
 * @see net.openhft.chronicle.map.ChronicleMapBuilder
 */
public final class ChronicleSetBuilder<E>
        implements ChronicleHashBuilder<E, ChronicleSet<E>, ChronicleSetBuilder<E>> {

    private ChronicleMapBuilder<E, DummyValue> chronicleMapBuilder;

    ChronicleSetBuilder(Class<E> keyClass) {
        chronicleMapBuilder = ChronicleMapBuilder.of(keyClass, DummyValue.class)
                .valueMarshallers(DummyValueMarshaller.INSTANCE, DummyValueMarshaller.INSTANCE)
                .valueSizeMarshaller(DummyValueMarshaller.INSTANCE);
    }

    public static <K> ChronicleSetBuilder<K> of(Class<K> keyClass) {
        return new ChronicleSetBuilder<>(keyClass);
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
    public ChronicleSetBuilder<E> entriesPerSegment(long entriesPerSegment) {
        chronicleMapBuilder.entriesPerSegment(entriesPerSegment);
        return this;
    }

    @Override
    public ChronicleSetBuilder<E> actualChunksPerSegment(long actualChunksPerSegment) {
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
    public ChronicleSetBuilder<E> averageKeySize(double averageKeySize) {
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
    public ChronicleSetBuilder<E> constantKeySizeBySample(E sampleKey) {
        chronicleMapBuilder.constantKeySizeBySample(sampleKey);
        return this;
    }

    @Override
    public ChronicleSetBuilder<E> actualChunkSize(int actualChunkSize) {
        chronicleMapBuilder.actualChunkSize(actualChunkSize);
        return this;
    }

    @Override
    public ChronicleSetBuilder<E> maxChunksPerEntry(int maxChunksPerEntry) {
        chronicleMapBuilder.maxChunksPerEntry(maxChunksPerEntry);
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
    public ChronicleSetBuilder<E> keyMarshaller(@NotNull BytesMarshaller<? super E> keyMarshaller) {
        chronicleMapBuilder.keyMarshaller(keyMarshaller);
        return this;
    }

    @Override
    public ChronicleSetBuilder<E> keyMarshallers(@NotNull BytesWriter<E> keyWriter,
                                                 @NotNull BytesReader<E> keyReader) {
        chronicleMapBuilder.keyMarshallers(keyWriter, keyReader);
        return this;
    }

    @Override
    public ChronicleSetBuilder<E> keySizeMarshaller(@NotNull SizeMarshaller keySizeMarshaller) {
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
    public ChronicleSetBuilder<E> replication(SingleChronicleHashReplication replication) {
        chronicleMapBuilder.replication(replication);
        return this;
    }

    @Override
    public ChronicleSetBuilder<E> replication(byte identifier, TcpTransportAndNetworkConfig tcpTransportAndNetwork) {
        chronicleMapBuilder.replication(identifier, tcpTransportAndNetwork);
        return this;
    }

    @Override
    public ChronicleSetBuilder<E> replication(byte identifier) {
        chronicleMapBuilder.replication(identifier);
        return this;
    }

    @Override
    public ChronicleHashInstanceBuilder<ChronicleSet<E>> instance() {
        return new SetInstanceBuilder<>(chronicleMapBuilder.instance());
    }

    @Override
    public ChronicleSet<E> create() {
        final ChronicleMap<E, DummyValue> map = chronicleMapBuilder.create();
        return new SetFromMap<>(map);
    }

    @Override
    public ChronicleSet<E> createPersistedTo(File file) throws IOException {
        ChronicleMap<E, DummyValue> map = chronicleMapBuilder.createPersistedTo(file);
        return new SetFromMap<>(map);
    }
}

