/*
 * Copyright 2014 Higher Frequency Trading http://www.higherfrequencytrading.com
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

import net.openhft.lang.io.serialization.BytesMarshaller;
import net.openhft.lang.io.serialization.BytesMarshallerFactory;
import net.openhft.lang.io.serialization.ObjectSerializer;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.map.Objects.builderEquals;

public class ChronicleSetBuilder<K> implements Cloneable {

    ChronicleMapBuilder<K, Boolean> chronicleMapBuilder;

    ChronicleSetBuilder(Class<K> keyClass) {
        chronicleMapBuilder = new ChronicleMapBuilder(keyClass, Boolean.class);
        chronicleMapBuilder.constantValueSizeBySample(Boolean.TRUE);
    }

    public static <K> ChronicleSetBuilder<K> of(Class<K> keyClass) {
        return new ChronicleSetBuilder(keyClass);
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


    public ChronicleSetBuilder<K> actualSegments(int actualSegments) {
        chronicleMapBuilder.actualSegments(actualSegments);
        return this;
    }

    public int actualSegments() {
        return chronicleMapBuilder.actualSegments();
    }

    /**
     * Set minimum number of segments. See concurrencyLevel in {@link java.util.concurrent.ConcurrentHashMap}.
     *
     * @param minSegments the minimum number of segments in maps, constructed by this builder
     * @return this builder object back
     */
    public ChronicleSetBuilder<K> minSegments(int minSegments) {
        chronicleMapBuilder.minSegments(minSegments);
        return this;
    }

    public int minSegments() {
        return chronicleMapBuilder.minSegments();
    }


    public ChronicleSetBuilder<K> actualElementsPerSegment(int actualEntriesPerSegment) {
        chronicleMapBuilder.actualEntriesPerSegment(actualEntriesPerSegment);
        return this;
    }

    public int actualElementsPerSegment() {
        return chronicleMapBuilder.actualEntriesPerSegment();
    }


    /**
     * Configures the optimal number of bytes for an element. If elementSize is always the same, call {@link
     * #constantElementsSizeBySample(Object)} method instead of this one.
     *
     * <p>If the size varies moderately, specify the size higher than average, but lower than the maximum
     * possible, to minimize average memory overuse. If the size varies in a wide range, it's better to use
     *
     *
     * <p>Example: if element in your set(s) are English words in {@link String} form, keys size 10
     * (a bit more than average English word length) would be a good choice: <pre>{@code
     * ChronicleSetBuilder<String, LongValue> wordFrequencies = ChronicleSetBuilder
     *     .of(String.class)
     *     .entries(50000)
     *     .elementSize(10)
     *     .create();}</pre>
     *
     * @param elementSize number of bytes, taken by serialized form of the element
     * @return this {@code ChronicleSetBuilder} back
     * @see #constantElementsSizeBySample(Object)
     */
    public ChronicleSetBuilder<K> elementSize(int elementSize) {
        chronicleMapBuilder.keySize(elementSize);
        return this;
    }


    /**
     * Configures the constant number of bytes, taken by serialized form of elements, created by this builder.
     * This is done by providing the {@code element}, all elements should take the same number of bytes in
     * serialized form, as this sample object.
     *
     * <p>For example, if your elements are Git commit hashes:<pre>{@code
     * Map<byte[], String> gitCommitMessagesByHash =
     *     ChronicleSetBuilder.of(byte[].class)
     *     .constantElementsSizeBySample(new byte[20])
     *     .create();
     * }</pre>
     *
     * <p>If elements are of boxed primitive type or {@link net.openhft.lang.model.Byteable} subclass, i. e.
     * if element size is known statically, it is automatically accounted and this method shouldn't be
     * called.
     *
     * @param element the sample element
     * @return this builder back
     */
    public ChronicleSetBuilder<K> constantElementsSizeBySample(K element) {
        chronicleMapBuilder.constantKeySizeBySample(element);
        return this;
    }


    /**
     * Configures alignment strategy of address in memory of elements
     *
     * <p>Useful when elements are updated intensively, particularly fields with volatile access, because it
     * doesn't work well if the value elements cache lines. Also, on some (nowadays rare) architectures any
     * misaligned memory access is more expensive than aligned.
     *
     * @param alignment the new alignment of the seta constructed by this builder
     * @return this {@code ChronicleSetBuilder} back
     */
    public ChronicleSetBuilder<K> alignment(Alignment alignment) {
        chronicleMapBuilder.entryAndValueAlignment(alignment);
        return this;
    }

    /**
     * * Returns the alignment memory strategy used for addresses of elements
     *
     * <p>Default is {@link net.openhft.chronicle.map.Alignment#OF_4_BYTES}.
     *
     * @return alignment memory strategy
     * @see #alignment(net.openhft.chronicle.map.Alignment)
     */
    public Alignment alignment() {
        return chronicleMapBuilder.entryAndValueAlignment();
    }

    public ChronicleSetBuilder<K> elements(long entries) {
        chronicleMapBuilder.entries(entries);
        return this;
    }

    public long elements() {
        return chronicleMapBuilder.entries();
    }

    public ChronicleSetBuilder<K> replicas(int replicas) {
        chronicleMapBuilder.replicas(replicas);
        return this;
    }

    public int replicas() {
        return chronicleMapBuilder.replicas();
    }


    /**
     * Not supported yet.
     *
     * @param transactional if the built map should be transactional
     * @return this {@code ChronicleMapBuilder} back
     */
    public ChronicleSetBuilder<K> transactional(boolean transactional) {
        chronicleMapBuilder.transactional(transactional);
        return this;
    }

    public boolean transactional() {
        return chronicleMapBuilder.transactional();
    }

    public ChronicleSetBuilder<K> lockTimeOut(long lockTimeOut, TimeUnit unit) {
        chronicleMapBuilder.lockTimeOut(lockTimeOut, unit);
        return this;
    }

    public long lockTimeOut(TimeUnit unit) {
        return chronicleMapBuilder.lockTimeOut(unit);
    }


    public ChronicleSetBuilder<K> errorListener(MapErrorListener errorListener) {
        chronicleMapBuilder.errorListener(errorListener);
        return this;
    }

    public MapErrorListener errorListener() {
        return chronicleMapBuilder.errorListener();
    }


    public boolean largeSegments() {
        return chronicleMapBuilder.largeSegments();
    }

    public ChronicleSetBuilder<K> largeSegments(boolean largeSegments) {
        chronicleMapBuilder.largeSegments(largeSegments);
        return this;
    }

    public ChronicleSetBuilder<K> metaDataBytes(int metaDataBytes) {
        chronicleMapBuilder.metaDataBytes(metaDataBytes);
        return this;
    }

    public int metaDataBytes() {
        return chronicleMapBuilder.metaDataBytes();
    }

    @Override
    public String toString() {
        return " ChronicleMapBuilder{" +
                "actualSegments=" + actualSegments() +
                ", minSegments=" + minSegments() +
                ", actualEntriesPerSegment=" + actualElementsPerSegment() +
                ", alignment=" + alignment() +
                ", entries=" + elements() +
                ", replicas=" + replicas() +
                ", transactional=" + transactional() +
                ", metaDataBytes=" + metaDataBytes() +
                ", errorListener=" + errorListener() +
                ", largeSegments=" + largeSegments() +
                ", timeProvider=" + timeProvider() +
                ", bytesMarshallerfactory=" + bytesMarshallerFactory() +
                '}';
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(Object o) {
        return builderEquals(this, o);
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    public ChronicleSetBuilder<K> addReplicator(Replicator replicator) {
        chronicleMapBuilder.addReplicator(replicator);
        return this;
    }

    public ChronicleSetBuilder<K> setReplicators(Replicator... replicators) {
        chronicleMapBuilder.setReplicators(replicators);
        return this;
    }

    public ChronicleSetBuilder<K> timeProvider(TimeProvider timeProvider) {
        chronicleMapBuilder.timeProvider(timeProvider);
        return this;
    }

    public TimeProvider timeProvider() {
        return chronicleMapBuilder.timeProvider();
    }

    public BytesMarshallerFactory bytesMarshallerFactory() {
        return chronicleMapBuilder.bytesMarshallerFactory();
    }

    public ChronicleSetBuilder<K> bytesMarshallerFactory(BytesMarshallerFactory bytesMarshallerFactory) {
        chronicleMapBuilder.bytesMarshallerFactory(bytesMarshallerFactory);
        return this;
    }

    public ObjectSerializer objectSerializer() {
        return chronicleMapBuilder.objectSerializer();
    }

    public ChronicleSetBuilder<K> objectSerializer(ObjectSerializer objectSerializer) {
        chronicleMapBuilder.objectSerializer(objectSerializer);
        return this;
    }

    /**
     * For testing
     */
    ChronicleSetBuilder<K> forceReplicatedImpl() {
        chronicleMapBuilder.forceReplicatedImpl();
        return this;
    }

    public ChronicleSetBuilder<K> keyMarshaller(@NotNull BytesMarshaller<K> keyMarshaller) {
        chronicleMapBuilder.keyMarshaller(keyMarshaller);
        return this;
    }

    /**
     * Specifies that elements, are immutable.  {@code ChronicleSet} implementations can benefit from the
     * knowledge that elements are not mutated between queries. By default, {@code ChronicleMapBuilder}
     * detects immutability automatically only for very few standard JDK types (for example, for {@link
     * String}), it is not recommended to rely on {@code ChronicleSetBuilder} to be smart enough about this.
     *
     * @return this builder back
     */
    public ChronicleSetBuilder<K> immutableKeys() {
        chronicleMapBuilder.immutableKeys();
        return this;
    }


    public Set<K> create(File file) throws IOException {
        final ChronicleMap<K, Boolean> map = chronicleMapBuilder.create(file);
        return Collections.newSetFromMap(map);
    }

    public Set<K> create() throws IOException {
        final ChronicleMap<K, Boolean> map = chronicleMapBuilder.create();
        return Collections.newSetFromMap(map);
    }


}

