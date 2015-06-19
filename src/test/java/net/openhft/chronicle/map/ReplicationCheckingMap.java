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

package net.openhft.chronicle.map;

import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

import java.io.File;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Rob Austin.
 */
public class ReplicationCheckingMap<K, V> implements ChronicleMap<K, V> {

    ChronicleMap<K, V> map1;
    ChronicleMap<K, V> map2;

    public ReplicationCheckingMap(ChronicleMap map1, ChronicleMap map2) {
        this.map1 = map1;
        this.map2 = map2;
    }

    @Override
    public V putIfAbsent(K key, V value) {
        return map1.putIfAbsent(key, value);
    }

    @Override
    public boolean remove(Object key, Object value) {
        return map1.remove(key, value);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        return map1.replace(key, oldValue, newValue);
    }

    @Override
    public V replace(final K key, final V value) {
        return check(new Call<K, V>() {
                         @Override
                         public Object method(ConcurrentMap<K, V> map) {
                             return map.replace(key, value);
                         }
                     }
        );
    }

    @Override
    public int size() {
        return check(new Call<K, V>() {
                         @Override
                         public Object method(ConcurrentMap<K, V> map) {
                             return map.size();
                         }
                     }
        );
    }

    public <R> R check(Call instance) {
        R r1 = null;
        R r2 = null;
        for (int i = 0; i < 50; i++) {
            r1 = (R) instance.method(map1);
            r2 = (R) instance.method(map2);

            if (r1 != null && r1.equals(r2))
                return r1;

            if (i > 30) {
                try {
                    Thread.sleep(i);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else
                Thread.yield();
        }

        Assert.assertEquals(map1, map2);
        System.out.print(map1);
        System.out.print(map2);

        if (r1 != null)
            Assert.assertEquals(r1.toString(), r2.toString());

        return (R) r1;
    }

    @Override
    public boolean isEmpty() {
        return check(new Call<K, V>() {
                         @Override
                         public Object method(ConcurrentMap<K, V> map) {
                             return map.isEmpty();
                         }
                     }
        );
    }

    @Override
    public boolean containsKey(final Object key) {
        return check(new Call<K, V>() {
                         @Override
                         public Object method(ConcurrentMap<K, V> map) {
                             return map.containsKey(key);
                         }
                     }
        );
    }

    @Override
    public boolean containsValue(final Object value) {
        return check(new Call<K, V>() {
                         @Override
                         public Object method(ConcurrentMap<K, V> map) {
                             return map.containsValue(value);
                         }
                     }
        );
    }

    @Override
    public V get(final Object key) {
        return check(new Call<K, V>() {
                         @Override
                         public Object method(ConcurrentMap<K, V> map) {
                             return map.get(key);
                         }
                     }
        );
    }

    @Override
    public V put(K key, V value) {
        return map1.put(key, value);
    }

    @Override
    public V remove(Object key) {
        return map1.remove(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        map1.putAll(m);
    }

    @Override
    public void clear() {
        map1.clear();
    }

    @NotNull
    @Override
    public Set<K> keySet() {
        return check(new Call<K, V>() {
                         @Override
                         public Object method(ConcurrentMap<K, V> map) {
                             return map.keySet();
                         }
                     }
        );
    }

    @NotNull
    @Override
    public Collection<V> values() {
        return check(new Call<K, V>() {
                         @Override
                         public Collection<V> method(ConcurrentMap<K, V> map) {
                             return map.values();
                         }
                     }
        );
    }

    @NotNull
    @Override
    public Set<Entry<K, V>> entrySet() {
        return check(new Call<K, V>() {
                         @Override
                         public Object method(ConcurrentMap<K, V> map) {
                             return (map.entrySet());
                         }
                     }
        );
    }

    @Override
    public long longSize() {
        return map1.longSize();
    }

    @Override
    public V getUsing(K key, V usingValue) {
        return map1.getUsing(key, usingValue);
    }

    @Override
    public <R> R getMapped(K key, @NotNull Function<? super V, R> function) {
        return map1.getMapped(key, function);
    }

    @Override
    public V putMapped(K key, @NotNull UnaryOperator<V> unaryOperator) {
        return map1.putMapped(key, unaryOperator);
    }

    @Override
    public void getAll(File toFile) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(File fromFile) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V newValueInstance() {
        throw new UnsupportedOperationException();
    }

    @Override
    public K newKeyInstance() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Class<K> keyClass() {
        return map1.keyClass();
    }

    @Override
    public Class<V> valueClass() {
        return map1.valueClass();
    }

    @Override
    public V acquireUsing(@NotNull K key, V usingValue) {
        return map1.acquireUsing(key, usingValue);
    }

    @NotNull
    @Override
    public WriteContext<K, V> acquireUsingLocked(@NotNull K key, @NotNull V usingValue) {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public ReadContext<K, V> getUsingLocked(@NotNull K key, @NotNull V usingValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public File file() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        map1.close();
        map2.close();
    }

    @Override
    public boolean equals(Object o) {
        return map1.equals(o);
    }

    @Override
    public int hashCode() {
        return map1.hashCode();
    }

    public String toString() {
        return map1.toString();
    }

    interface Call<K, V> {
        Object method(ConcurrentMap<K, V> map);
    }

}
