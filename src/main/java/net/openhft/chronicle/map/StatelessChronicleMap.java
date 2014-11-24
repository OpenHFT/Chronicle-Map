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

package net.openhft.chronicle.map;

import net.openhft.lang.thread.NamedThreadFactory;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author Rob Austin.
 */
class StatelessChronicleMap<K, V> implements ChronicleMap<K, V>, Closeable, Cloneable {


    private final AbstractChronicleMapBuilder chronicleMapBuilder;
    private final StatelessMapConfig builder;
    private final ChronicleMap<K, V> statelessClient;

    private ChronicleMap<K, V> asyncStatelessClient;
    private ExecutorService executorService;

    public StatelessChronicleMap(final StatelessMapConfig builder,
                                 final AbstractChronicleMapBuilder chronicleMapBuilder) throws IOException {
        this.builder = builder;
        this.chronicleMapBuilder = chronicleMapBuilder;
        statelessClient = new StatelessChronicleMapUnsynchronized(builder, chronicleMapBuilder);

    }


    @Override
    public Future<V> getLater(@NotNull final K key) {
        return lazyExecutorService().submit(new Callable<V>() {
            @Override
            public V call() throws Exception {
                return lazyAsyncStatelessClient().get(key);
            }
        });
    }

    @Override
    public Future<V> putLater(@NotNull final K key, @NotNull final V value) {
        return lazyExecutorService().submit(new Callable<V>() {
            @Override
            public V call() throws Exception {
                return lazyAsyncStatelessClient().put(key, value);
            }
        });
    }


    private ExecutorService lazyExecutorService() {

        if (executorService == null) {
            synchronized (this) {
                if (executorService == null)
                    executorService = Executors.newSingleThreadExecutor(new NamedThreadFactory(chronicleMapBuilder.name() +
                            "-stateless-client-async",
                            true));
            }
        }

        return executorService;
    }


    /**
     * @return a lazily created instance of the StatelessClient used for the async calls.
     */
    private ChronicleMap<K, V> lazyAsyncStatelessClient() {

        if (asyncStatelessClient == null) {
            synchronized (this) {
                if (asyncStatelessClient == null)
                    try {
                        asyncStatelessClient = new StatelessChronicleMapUnsynchronized<K, V>(builder,
                                chronicleMapBuilder);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
            }
        }

        return asyncStatelessClient;
    }


    /**
     * calling this method should be avoided at all cost, as the entire {@code object} is serialized. This
     * equals can be used to compare map that extends ChronicleMap.  So two Chronicle Maps that contain the
     * same data are considered equal, even if the instances of the chronicle maps were of different types
     *
     * @param object the object that you are comparing against
     * @return true if the contain the same data
     */
    @Override
    public boolean equals(Object object) {
        return statelessClient.equals(object);
    }


    @Override
    public int hashCode() {
        return statelessClient.hashCode();
    }

    @Override
    public String toString() {
        return statelessClient.toString();
    }

    @Override
    public synchronized long longSize() {
        return statelessClient.longSize();
    }

    @Override
    public synchronized int size() {
        return statelessClient.size();
    }

    @Override
    public synchronized boolean isEmpty() {
        return statelessClient.isEmpty();
    }

    @Override
    public synchronized boolean containsKey(Object key) {
        return statelessClient.containsKey(key);
    }

    @Override
    public synchronized boolean containsValue(Object value) {
        return statelessClient.containsValue(value);
    }

    @Override
    public synchronized V get(Object key) {
        return statelessClient.get(key);

    }

    @Override
    public synchronized V put(K key, V value) {
        return statelessClient.put(key, value);
    }

    @Override
    public synchronized V remove(Object key) {
        return statelessClient.remove(key);
    }

    @Override
    public synchronized void putAll(Map<? extends K, ? extends V> m) {
        statelessClient.putAll(m);
    }

    @Override
    public synchronized void clear() {
        statelessClient.clear();
    }

    @NotNull
    @Override
    public synchronized Set<K> keySet() {
        return statelessClient.keySet();
    }

    @NotNull
    @Override
    public synchronized Collection<V> values() {
        return statelessClient.values();
    }

    @NotNull
    @Override
    public synchronized Set<Entry<K, V>> entrySet() {
        return statelessClient.entrySet();
    }

    @Override
    public synchronized V getUsing(K key, V usingValue) {
        return statelessClient.getUsing(key, usingValue);
    }

    @NotNull
    @Override
    public synchronized ReadContext<K, V> getUsingLocked(@NotNull K key, @NotNull V usingValue) {
        return statelessClient.getUsingLocked(key, usingValue);
    }

    @Override
    public synchronized V acquireUsing(@NotNull K key, V usingValue) {
        return statelessClient.acquireUsing(key, usingValue);
    }

    @NotNull
    @Override
    public synchronized WriteContext<K, V> acquireUsingLocked(@NotNull K key, @NotNull V usingValue) {
        return statelessClient.acquireUsingLocked(key, usingValue);
    }

    @Override
    public synchronized <R> R mapForKey(K key, @NotNull Function<? super V, R> function) {
        return statelessClient.mapForKey(key, function);
    }

    @Override
    public synchronized <R> R updateForKey(K key, @NotNull Mutator<? super V, R> mutator) {
        return statelessClient.updateForKey(key, mutator);
    }


    @Override
    public File file() {
        return statelessClient.file();
    }

    @Override
    public void close() {
        statelessClient.close();
    }

    @Override
    public V putIfAbsent(K key, V value) {
        return statelessClient.putIfAbsent(key, value);
    }

    @Override
    public boolean remove(Object key, Object value) {
        return statelessClient.remove(key, value);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        return statelessClient.replace(key, oldValue, newValue);
    }

    @Override
    public V replace(K key, V value) {
        return statelessClient.replace(key, value);
    }
}

