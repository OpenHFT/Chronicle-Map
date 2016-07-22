/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
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
    public void close() {
        m.close();
    }

    @Override
    public boolean isOpen() {
        return m.isOpen();
    }
}
