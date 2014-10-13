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

import net.openhft.chronicle.ChronicleHashBuilder;
import net.openhft.chronicle.ChronicleHashErrorListener;
import net.openhft.chronicle.TimeProvider;
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


public class ChronicleSetBuilder<E>
        implements ChronicleHashBuilder<E, ChronicleSet<E>, ChronicleSetBuilder<E>> {

    private ChronicleMapBuilder<E, Void> chronicleMapBuilder;

    ChronicleSetBuilder(Class<E> keyClass) {
        chronicleMapBuilder = ChronicleMapBuilder.of(keyClass, Void.class)
                .entryAndValueAlignment(Alignment.NO_ALIGNMENT);
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
    public ChronicleSetBuilder<E> actualEntriesPerSegment(int actualEntriesPerSegment) {
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
     * (Note that 10 is chosen as key size in bytes despite strings in Java are UTF-16 encoded (and each
     * character takes 2 bytes on-heap), because default off-heap {@link String} encoding is UTF-8 in {@code
     * ChronicleSet}.)
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
     * <p>In fully default case you can expect entry size to be about 120-130 bytes. But it is strongly
     * recommended always to configure {@linkplain #keySize(int) key size}, if they couldn't be derived
     * statically.
     *
     * <p>If entry size is not configured explicitly by calling this method, it is computed based on
     * {@linkplain #metaDataBytes(int) meta data bytes}, plus {@linkplain #keySize(int) key size}, plus a few
     * bytes required by implementations.
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
    public ChronicleSetBuilder<E> largeSegments(boolean largeSegments) {
        chronicleMapBuilder.largeSegments(largeSegments);
        return this;
    }

    @Override
    public ChronicleSetBuilder<E> metaDataBytes(int metaDataBytes) {
        chronicleMapBuilder.metaDataBytes(metaDataBytes);
        return this;
    }

    @Override
    public ChronicleSetBuilder<E> replicators(byte identifier, Replicator... replicators) {
        chronicleMapBuilder.replicators(identifier, replicators);
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

    public ChronicleSetBuilder<E> replicators(byte identifier, Replicator replicator) {
        chronicleMapBuilder.replicators(identifier, replicator);
        return this;
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

    @Override
    public ChronicleSetBuilder<E> immutableKeys() {
        chronicleMapBuilder.immutableKeys();
        return this;
    }

    @Override
    public ChronicleSet<E> create(File file) throws IOException {
        final ChronicleMap<E, Void> map = chronicleMapBuilder.create(file);
        return new SetFromMap<E>(map);
    }

    @Override
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

