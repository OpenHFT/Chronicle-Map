/*
 *      Copyright (C) 2015  higherfrequencytrading.com
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.set;

import net.openhft.chronicle.hash.ChronicleHashBuilder;
import net.openhft.chronicle.hash.ChronicleHashBuilderPrivateAPI;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.serialization.*;
import net.openhft.chronicle.map.*;
import net.openhft.chronicle.map.replication.MapRemoteOperations;
import net.openhft.chronicle.map.replication.MapRemoteQueryContext;
import net.openhft.chronicle.set.replication.SetRemoteOperations;
import net.openhft.chronicle.set.replication.SetRemoteQueryContext;
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
                .valueReaderAndDataAccess(
                        DummyValueMarshaller.INSTANCE, DummyValueMarshaller.INSTANCE)
                .valueSizeMarshaller(SizeMarshaller.constant(0));
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
    public ChronicleSetBuilder<K> actualChunksPerSegmentTier(long actualChunksPerSegmentTier) {
        chronicleMapBuilder.actualChunksPerSegmentTier(actualChunksPerSegmentTier);
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
     * @param averageKeySize   the average size in bytes of the key
     */
    @Override
    public ChronicleSetBuilder<K> averageKeySize(double averageKeySize) {
        chronicleMapBuilder.averageKeySize(averageKeySize);
        return this;
    }

    @Override
    public ChronicleSetBuilder<K> averageKey(K averageKey) {
        chronicleMapBuilder.averageKey(averageKey);
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
    public ChronicleSetBuilder<K> maxBloatFactor(double maxBloatFactor) {
        chronicleMapBuilder.maxBloatFactor(maxBloatFactor);
        return this;
    }

    @Override
    public ChronicleSetBuilder<K> allowSegmentTiering(boolean allowSegmentTiering) {
        chronicleMapBuilder.allowSegmentTiering(allowSegmentTiering);
        return this;
    }

    @Override
    public ChronicleSetBuilder<K> nonTieredSegmentsPercentile(double nonTieredSegmentsPercentile) {
        chronicleMapBuilder.nonTieredSegmentsPercentile(nonTieredSegmentsPercentile);
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
    public ChronicleSetBuilder<K> removedEntryCleanupTimeout(
            long removedEntryCleanupTimeout, TimeUnit unit) {
        chronicleMapBuilder.removedEntryCleanupTimeout(removedEntryCleanupTimeout, unit);
        return this;
    }

    @Override
    public ChronicleSetBuilder<K> cleanupRemovedEntries(boolean cleanupRemovedEntries) {
        chronicleMapBuilder.cleanupRemovedEntries(cleanupRemovedEntries);
        return this;
    }

    @Override
    public ChronicleSetBuilder<K> keyReaderAndDataAccess(
            SizedReader<K> keyReader, @NotNull DataAccess<K> keyDataAccess) {
        chronicleMapBuilder.keyReaderAndDataAccess(keyReader, keyDataAccess);
        return this;
    }

    @Override
    public ChronicleSetBuilder<K> keyMarshallers(
            @NotNull BytesReader<K> keyReader, @NotNull BytesWriter<? super K> keyWriter) {
        chronicleMapBuilder.keyMarshallers(keyReader, keyWriter);
        return this;
    }

    @Override
    public <M extends BytesReader<K> & BytesWriter<? super K>>
    ChronicleSetBuilder<K> keyMarshaller(@NotNull M marshaller) {
        chronicleMapBuilder.keyMarshaller(marshaller);
        return this;
    }

    @Override
    public ChronicleSetBuilder<K> keyMarshallers(
            @NotNull SizedReader<K> keyReader, @NotNull SizedWriter<? super K> keyWriter) {
        chronicleMapBuilder.keyMarshallers(keyReader, keyWriter);
        return this;
    }

    @Override
    public <M extends SizedReader<K> & SizedWriter<? super K>>
    ChronicleSetBuilder<K> keyMarshaller(@NotNull M sizedMarshaller) {
        chronicleMapBuilder.keyMarshaller(sizedMarshaller);
        return this;
    }

    @Override
    public ChronicleSetBuilder<K> keySizeMarshaller(@NotNull SizeMarshaller keySizeMarshaller) {
        chronicleMapBuilder.keySizeMarshaller(keySizeMarshaller);
        return this;
    }

    @Override
    public ChronicleSetBuilder<K> aligned64BitMemoryOperationsAtomic(
            boolean aligned64BitMemoryOperationsAtomic) {
        chronicleMapBuilder.aligned64BitMemoryOperationsAtomic(aligned64BitMemoryOperationsAtomic);
        return this;
    }

    @Override
    public ChronicleSetBuilder<K> checksumEntries(boolean checksumEntries) {
        chronicleMapBuilder.checksumEntries(checksumEntries);
        return this;
    }

    @Override
    public ChronicleSetBuilder<K> replication(byte identifier) {
        chronicleMapBuilder.replication(identifier);
        return this;
    }

    public ChronicleSetBuilder<K> entryOperations(SetEntryOperations<K, ?> entryOperations) {
        chronicleMapBuilder.entryOperations(new MapEntryOperations<K, DummyValue, Object>() {
            @Override
            public Object remove(@NotNull MapEntry<K, DummyValue> entry) {
                return entryOperations.remove((SetEntry<K>) entry);
            }

            @Override
            public Object insert(
                    @NotNull MapAbsentEntry<K, DummyValue> absentEntry, Data<DummyValue> value) {
                return entryOperations.insert((SetAbsentEntry<K>) absentEntry);
            }
        });
        return this;
    }

    public ChronicleSetBuilder<K> remoteOperations(SetRemoteOperations<K, ?> remoteOperations) {
        chronicleMapBuilder.remoteOperations(new MapRemoteOperations<K, DummyValue, Object>() {
            @Override
            public void remove(MapRemoteQueryContext<K, DummyValue, Object> q) {
                remoteOperations.remove((SetRemoteQueryContext) q);
            }

            @Override
            public void put(
                    MapRemoteQueryContext<K, DummyValue, Object> q, Data<DummyValue> newValue) {
                remoteOperations.put((SetRemoteQueryContext) q);
            }
        });
        return this;
    }

    @Override
    public ChronicleSet<K> create() {
        final ChronicleMap<K, DummyValue> map = chronicleMapBuilder.create();
        return new SetFromMap<>((VanillaChronicleMap<K, DummyValue, ?>) map);
    }

    @Override
    public ChronicleSet<K> createPersistedTo(File file) throws IOException {
        ChronicleMap<K, DummyValue> map = chronicleMapBuilder.createPersistedTo(file);
        return new SetFromMap<>((VanillaChronicleMap<K, DummyValue, ?>) map);
    }

    @Override
    public ChronicleSet<K> recoverPersistedTo(File file, boolean sameBuilderConfig)
            throws IOException {
        ChronicleMap<K, DummyValue> map =
                chronicleMapBuilder.recoverPersistedTo(file, sameBuilderConfig);
        return new SetFromMap<>((VanillaChronicleMap<K, DummyValue, ?>) map);
    }

    /**
     * @deprecated don't use private API in the client code
     */
    @Deprecated
    @Override
    public ChronicleHashBuilderPrivateAPI<K> privateAPI() {
        //noinspection deprecation
        return chronicleMapBuilder.privateAPI();
    }
}

