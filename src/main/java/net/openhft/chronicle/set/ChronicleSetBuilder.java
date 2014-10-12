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

package net.openhft.chronicle.set;

import net.openhft.chronicle.map.*;
import net.openhft.lang.io.serialization.BytesMarshaller;
import net.openhft.lang.io.serialization.BytesMarshallerFactory;
import net.openhft.lang.io.serialization.ObjectSerializer;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;


public class ChronicleSetBuilder<E> implements Cloneable {

    private ChronicleMapBuilder<E, Void> chronicleMapBuilder;

    ChronicleSetBuilder(Class<E> keyClass) {
        chronicleMapBuilder = ChronicleMapBuilder.of(keyClass, Void.class);
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

    public ChronicleSetBuilder<E> actualSegments(int actualSegments) {
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
    public ChronicleSetBuilder<E> minSegments(int minSegments) {
        chronicleMapBuilder.minSegments(minSegments);
        return this;
    }

    public int minSegments() {
        return chronicleMapBuilder.minSegments();
    }

    public ChronicleSetBuilder<E> actualElementsPerSegment(int actualEntriesPerSegment) {
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
    public ChronicleSetBuilder<E> elementSize(int elementSize) {
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
    public ChronicleSetBuilder<E> constantElementsSizeBySample(E element) {
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
    public ChronicleSetBuilder<E> alignment(Alignment alignment) {
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

    public ChronicleSetBuilder<E> elements(long entries) {
        chronicleMapBuilder.entries(entries);
        return this;
    }

    public long elements() {
        return chronicleMapBuilder.entries();
    }

    public ChronicleSetBuilder<E> replicas(int replicas) {
        chronicleMapBuilder.replicas(replicas);
        return this;
    }

    public int replicas() {
        return chronicleMapBuilder.replicas();
    }


    public ChronicleSetBuilder<E> lockTimeOut(long lockTimeOut, TimeUnit unit) {
        chronicleMapBuilder.lockTimeOut(lockTimeOut, unit);
        return this;
    }

    public long lockTimeOut(TimeUnit unit) {
        return chronicleMapBuilder.lockTimeOut(unit);
    }


    public ChronicleSetBuilder<E> errorListener(MapErrorListener errorListener) {
        chronicleMapBuilder.errorListener(errorListener);
        return this;
    }

    public MapErrorListener errorListener() {
        return chronicleMapBuilder.errorListener();
    }


    public boolean largeSegments() {
        return chronicleMapBuilder.largeSegments();
    }

    public ChronicleSetBuilder<E> largeSegments(boolean largeSegments) {
        chronicleMapBuilder.largeSegments(largeSegments);
        return this;
    }

    public ChronicleSetBuilder<E> metaDataBytes(int metaDataBytes) {
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
        return chronicleMapBuilder.equals(o);
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    public ChronicleSetBuilder<E> addReplicator(Replicator replicator) {
        chronicleMapBuilder.addReplicator(replicator);
        return this;
    }

    public ChronicleSetBuilder<E> setReplicators(Replicator... replicators) {
        chronicleMapBuilder.setReplicators(replicators);
        return this;
    }

    public ChronicleSetBuilder<E> timeProvider(TimeProvider timeProvider) {
        chronicleMapBuilder.timeProvider(timeProvider);
        return this;
    }

    public TimeProvider timeProvider() {
        return chronicleMapBuilder.timeProvider();
    }

    public BytesMarshallerFactory bytesMarshallerFactory() {
        return chronicleMapBuilder.bytesMarshallerFactory();
    }

    public ChronicleSetBuilder<E> bytesMarshallerFactory(BytesMarshallerFactory bytesMarshallerFactory) {
        chronicleMapBuilder.bytesMarshallerFactory(bytesMarshallerFactory);
        return this;
    }

    public ObjectSerializer objectSerializer() {
        return chronicleMapBuilder.objectSerializer();
    }

    public ChronicleSetBuilder<E> objectSerializer(ObjectSerializer objectSerializer) {
        chronicleMapBuilder.objectSerializer(objectSerializer);
        return this;
    }

    public ChronicleSetBuilder<E> keyMarshaller(@NotNull BytesMarshaller<E> keyMarshaller) {
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
    public ChronicleSetBuilder<E> immutableKeys() {
        chronicleMapBuilder.immutableKeys();
        return this;
    }


    public ChronicleSet<E> create(File file) throws IOException {
        final ChronicleMap<E, Void> map = chronicleMapBuilder.create(file);
        return new SetFromMap<E>(map);
    }

    public ChronicleSet<E> create() throws IOException {
        final ChronicleMap<E, Void> map = chronicleMapBuilder.create();
        return new SetFromMap<E>(map);
    }

    private static class SetFromMap<E> extends AbstractSet<E>
            implements ChronicleSet<E>, Serializable {

        private final ChronicleMap<E, Void> m;  // The backing map
        private transient Set<E> s;       // Its keySet

        SetFromMap(ChronicleMap<E, Void> map) {
            if (!map.isEmpty())
                throw new IllegalArgumentException("Map is non-empty");
            m = map;
            s = map.keySet();
        }

        public void clear() {
            m.clear();
        }

        public int size() {
            return m.size();
        }

        public boolean isEmpty() {
            return m.isEmpty();
        }

        public boolean contains(Object o) {
            return m.containsKey(o);
        }

        public boolean remove(Object o) {
            return m.remove(o) != null;
        }

        public boolean add(E e) {
            return m.put(e, (Void) null) == null;
        }

        public Iterator<E> iterator() {
            return s.iterator();
        }

        public Object[] toArray() {
            return s.toArray();
        }

        public <T> T[] toArray(T[] a) {
            return s.toArray(a);
        }

        public String toString() {
            return s.toString();
        }

        public int hashCode() {
            return s.hashCode();
        }

        public boolean equals(Object o) {
            return o == this || s.equals(o);
        }

        public boolean containsAll(Collection<?> c) {
            return s.containsAll(c);
        }

        public boolean removeAll(Collection<?> c) {
            return s.removeAll(c);
        }

        public boolean retainAll(Collection<?> c) {
            return s.retainAll(c);
        }
        // addAll is the only inherited implementation

        private static final long serialVersionUID = 2454657854757543876L;

        private void readObject(java.io.ObjectInputStream stream)
                throws IOException, ClassNotFoundException {
            stream.defaultReadObject();
            s = m.keySet();
        }

        @Override
        public long longSize() {
            return m.longSize();
        }

        @Override
        public void close() throws IOException {
            m.close();
        }
    }

}

