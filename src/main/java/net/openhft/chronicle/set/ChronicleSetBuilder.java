/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.set;

import net.openhft.chronicle.hash.ChronicleHashBuilder;
import net.openhft.chronicle.hash.ChronicleHashBuilderPrivateAPI;
import net.openhft.chronicle.hash.ChronicleHashCorruption;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.serialization.*;
import net.openhft.chronicle.map.*;
import net.openhft.chronicle.map.replication.MapRemoteOperations;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * {@code ChronicleSetBuilder} manages the whole set of {@link ChronicleSet} configurations, could
 * be used as a classic builder and/or factory.
 * <p>
 * <p>{@code ChronicleMapBuilder} is mutable, see a note in {@link
 * ChronicleHashBuilder} interface documentation.
 *
 * @param <K> element type of the sets, created by this builder
 * @see ChronicleSet
 * @see ChronicleMapBuilder
 */
public final class ChronicleSetBuilder<K>
        implements ChronicleHashBuilder<K, ChronicleSet<K>, ChronicleSetBuilder<K>> {

    private static final Logger chronicleSetLogger = LoggerFactory.getLogger(ChronicleSet.class);
    private static final ChronicleHashCorruption.Listener defaultChronicleSetCorruptionListener =
            corruption -> {
                if (corruption.exception() != null) {
                    chronicleSetLogger.error(corruption.message(), corruption.exception());
                } else {
                    chronicleSetLogger.error(corruption.message());
                }
            };

    private ChronicleMapBuilder<K, DummyValue> chronicleMapBuilder;
    private ChronicleSetBuilderPrivateAPI<K> privateAPI;

    ChronicleSetBuilder(Class<K> keyClass) {
        chronicleMapBuilder = ChronicleMapBuilder.of(keyClass, DummyValue.class)
                .valueReaderAndDataAccess(
                        DummyValueMarshaller.INSTANCE, DummyValueMarshaller.INSTANCE)
                .valueSizeMarshaller(SizeMarshaller.constant(0));
        //noinspection deprecation,unchecked
        privateAPI = new ChronicleSetBuilderPrivateAPI<>(
                (ChronicleHashBuilderPrivateAPI<K, MapRemoteOperations<K, DummyValue, ?>>)
                        chronicleMapBuilder.privateAPI());
    }

    /**
     * Returns a new {@code ChronicleSetBuilder} instance which is able to {@linkplain #create()
     * create} sets with the specified key class.
     *
     * @param keyClass class object used to infer key type and discover it's properties via
     *                 reflection
     * @param <K>      key type of the sets, created by the returned builder
     * @return a new builder for the given key class
     */
    public static <K> ChronicleSetBuilder<K> of(Class<K> keyClass) {
        return new ChronicleSetBuilder<>(keyClass);
    }

    @Override
    public ChronicleSetBuilder<K> clone() {
        try {
            @SuppressWarnings("unchecked") final ChronicleSetBuilder<K> result = (ChronicleSetBuilder<K>) super.clone();
            result.chronicleMapBuilder = chronicleMapBuilder.clone();
            return result;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public ChronicleSetBuilder<K> name(String name) {
        chronicleMapBuilder.name(name);
        return this;
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
     * <p>
     * <p>Example: if keys in your set(s) are English words in {@link String} form, average English
     * word length is 5.1, configure average key size of 6: <pre>{@code
     * ChronicleSet<String> uniqueWords = ChronicleSetBuilder.of(String.class)
     *     .entries(50000)
     *     .averageKeySize(6)
     *     .create();}</pre>
     * <p>
     * <p>(Note that 6 is chosen as average key size in bytes despite strings in Java are UTF-16
     * encoded (and each character takes 2 bytes on-heap), because default off-heap {@link String}
     * encoding is UTF-8 in {@code ChronicleSet}.)
     *
     * @param averageKeySize the average size in bytes of the key
     * @see #constantKeySizeBySample(Object)
     * @see #actualChunkSize(int)
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
     * <p>
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

    /**
     * Inject your SPI code around basic {@code ChronicleSet}'s operations with entries:
     * removing entries and inserting new entries.
     * <p>
     * <p>This affects behaviour of ordinary set.add(), set.remove(), calls, as well as removes
     * <i>during iterations</i>, updates during <i>remote calls</i> and
     * <i>internal replication operations</i>.
     */
    public ChronicleSetBuilder<K> entryOperations(SetEntryOperations<K, ?> entryOperations) {
        chronicleMapBuilder.entryOperations(new MapEntryOperations<K, DummyValue, Object>() {
            @Override
            public Object remove(@NotNull MapEntry<K, DummyValue> entry) {
                //noinspection unchecked
                return entryOperations.remove((SetEntry<K>) entry);
            }

            @Override
            public Object insert(
                    @NotNull MapAbsentEntry<K, DummyValue> absentEntry, Data<DummyValue> value) {
                //noinspection unchecked
                return entryOperations.insert((SetAbsentEntry<K>) absentEntry);
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
    public ChronicleSet<K> createOrRecoverPersistedTo(File file) throws IOException {
        return createOrRecoverPersistedTo(file, true);
    }

    @Override
    public ChronicleSet<K> createOrRecoverPersistedTo(File file, boolean sameLibraryVersion)
            throws IOException {
        return createOrRecoverPersistedTo(file, sameLibraryVersion,
                defaultChronicleSetCorruptionListener);
    }

    @Override
    public ChronicleSet<K> createOrRecoverPersistedTo(
            File file, boolean sameLibraryVersion,
            ChronicleHashCorruption.Listener corruptionListener) throws IOException {
        ChronicleMap<K, DummyValue> map = chronicleMapBuilder.createOrRecoverPersistedTo(
                file, sameLibraryVersion, corruptionListener);
        return new SetFromMap<>((VanillaChronicleMap<K, DummyValue, ?>) map);
    }

    @Override
    public ChronicleSet<K> recoverPersistedTo(File file, boolean sameBuilderConfigAndLibraryVersion)
            throws IOException {
        return recoverPersistedTo(file, sameBuilderConfigAndLibraryVersion,
                defaultChronicleSetCorruptionListener);
    }

    @Override
    public ChronicleSet<K> recoverPersistedTo(
            File file, boolean sameBuilderConfigAndLibraryVersion,
            ChronicleHashCorruption.Listener corruptionListener) throws IOException {
        ChronicleMap<K, DummyValue> map = chronicleMapBuilder.recoverPersistedTo(
                file, sameBuilderConfigAndLibraryVersion, corruptionListener);
        return new SetFromMap<>((VanillaChronicleMap<K, DummyValue, ?>) map);
    }

    @Override
    public ChronicleSetBuilder<K> setPreShutdownAction(Runnable preShutdownAction) {
        chronicleMapBuilder.setPreShutdownAction(preShutdownAction);
        return this;
    }

    @Override
    public ChronicleSetBuilder<K> skipCloseOnExitHook(boolean skipCloseOnExitHook) {
        chronicleMapBuilder.skipCloseOnExitHook(skipCloseOnExitHook);
        return this;
    }

    /**
     * @deprecated don't use private API in the client code
     */
    @Deprecated
    @Override
    public Object privateAPI() {
        return privateAPI;
    }
}

