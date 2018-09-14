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

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.util.Objects;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.VanillaChronicleMap;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static net.openhft.chronicle.set.DummyValue.DUMMY_VALUE;

/**
 * ChronicleSet is implemented through ChronicleMap, with dummy 0-bytes values. This solution
 * trades correctness of abstractions for simplicity and minimizing changes before production 3.x
 * release.
 */
class SetFromMap<E> extends AbstractSet<E> implements ChronicleSet<E> {

    private final ChronicleMap<E, DummyValue> m;  // The backing map
    private transient Set<E> s;       // Its keySet

    SetFromMap(VanillaChronicleMap<E, DummyValue, ?> map) {
        m = map;
        map.chronicleSet = this;
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
        return m.remove(o, DUMMY_VALUE);
    }

    public boolean add(E e) {
        return m.putIfAbsent(e, DUMMY_VALUE) == null;
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

    @Override
    public String toIdentityString() {
        return "ChronicleSet{" +
                "name=" + name() +
                ", file=" + file() +
                ", identityHashCode=" + System.identityHashCode(this) +
                "}";
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

    @Override
    public long longSize() {
        return m.longSize();
    }

    @Override
    public long offHeapMemoryUsed() {
        return m.offHeapMemoryUsed();
    }

    @Override
    public Class<E> keyClass() {
        return m.keyClass();
    }

    // TODO test queryContext methods

    @NotNull
    @Override
    public ExternalSetQueryContext<E, ?> queryContext(E key) {
        //noinspection unchecked
        return (ExternalSetQueryContext<E, ?>) m.queryContext(key);
    }

    @NotNull
    @Override
    public ExternalSetQueryContext<E, ?> queryContext(Data<E> key) {
        //noinspection unchecked
        return (ExternalSetQueryContext<E, ?>) m.queryContext(key);
    }

    @NotNull
    @Override
    public ExternalSetQueryContext<E, ?> queryContext(BytesStore keyBytes, long offset, long size) {
        //noinspection unchecked
        return (ExternalSetQueryContext<E, ?>) m.queryContext(keyBytes, offset, size);
    }

    @Override
    public SetSegmentContext<E, ?> segmentContext(int segmentIndex) {
        // TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public int segments() {
        return m.segments();
    }

    // TODO test forEach methods

    @Override
    public boolean forEachEntryWhile(Predicate<? super SetEntry<E>> predicate) {
        Objects.requireNonNull(predicate);
        return m.forEachEntryWhile(e -> predicate.test(((SetEntry<E>) e)));
    }

    @Override
    public void forEachEntry(Consumer<? super SetEntry<E>> action) {
        Objects.requireNonNull(action);
        m.forEachEntry(e -> action.accept(((SetEntry<E>) e)));
    }

    @Override
    public File file() {
        return m.file();
    }

    @Override
    public String name() {
        return m.name();
    }

    @Override
    public void close() {
        m.close();
    }

    @Override
    public boolean isOpen() {
        return m.isOpen();
    }
}
