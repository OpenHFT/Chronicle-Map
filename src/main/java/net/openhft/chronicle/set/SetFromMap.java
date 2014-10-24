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

import net.openhft.chronicle.map.ChronicleMap;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import static net.openhft.chronicle.set.DummyValue.DUMMY_VALUE;

class SetFromMap<E> extends AbstractSet<E>
        implements ChronicleSet<E>, Serializable {

    private final ChronicleMap<E, DummyValue> m;  // The backing map
    private transient Set<E> s;       // Its keySet

    SetFromMap(ChronicleMap<E, DummyValue> map) {
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
        return m.remove(o) == DUMMY_VALUE;
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
    public File file() {
        return m.file();
    }

    @Override
    public void close() throws IOException {
        m.close();
    }
}
