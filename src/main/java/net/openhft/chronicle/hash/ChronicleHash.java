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

package net.openhft.chronicle.hash;

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.MapMethods;
import net.openhft.chronicle.map.MapQueryContext;
import net.openhft.chronicle.set.ChronicleSet;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Common base interface for {@link ChronicleMap} and {@link ChronicleSet}.
 */
public interface ChronicleHash<K, E extends HashEntry<K>, SC extends HashSegmentContext<K, ?>,
        EQC extends ExternalHashQueryContext<K>> extends Closeable {
    /**
     * Returns the file this hash container mapped to, i. e. when it is created by
     * {@link ChronicleHashBuilder#create()} call, or {@code null} if it is purely in-memory,
     * i. e. if it is created by {@link ChronicleHashBuilder#create()} call.
     *
     * @return the file this {@link ChronicleMap} or {@link ChronicleSet} is mapped to,
     * or {@code null} if it is not mapped to any file
     * @see ChronicleHashBuilder#createPersistedTo(File)
     */
    File file();

    /**
     * Returns the name of this {@code ChronicleHash}, configured by {@link
     * ChronicleHashBuilder#name(String)}, or {@code null}, if not configured.
     *
     * @return the name of this this {@link ChronicleMap} or {@link ChronicleSet}
     */
    String name();

    /**
     * Returns a {@code String}, useful for identifying this {@code ChronicleHash} in debugging,
     * logging, and error reporting. {@link #toString()} of concrete {@code ChronicleHash}
     * subinterfaces, {@link ChronicleMap} and {@link ChronicleSet}, has to follow {@link
     * Map#toString()} and {@link Set#toString()} contracts respectively, making it not always
     * useful (or even impossible to use, if this {@code ChronicleHash} contains a lot of entries)
     * for the purposes listed above.
     * <p>
     * <p>This method return a string of the form:<br><br>
     * [ChronicleMap|ChronicleSet]{name={@link #name()}, file={@link #file()},
     * identityHashCode={@link System#identityHashCode System.identityHashCode(thisChronicleHash)}}
     * <br><br>
     * This form could be changed in any subsequent Chronicle Map library release (including patch
     * release). The user code shouldn't depend on this form.
     *
     * @return a {@code String}, useful for identifying this {@code ChronicleHash} in debugging,
     * logging, and error reporting
     */
    String toIdentityString();

    /**
     * Returns the number of entries in this store.
     *
     * @return the number of entries in this store
     */
    long longSize();

    /**
     * Returns the amount of off-heap memory (in bytes), allocated by this {@code ChronicleHash} or
     * shared with with other ChronicleHashes, persisting to the same {@link #file()}.
     * <p>
     * <p>After {@link #close()} this method returns 0.
     *
     * @return the amount of off-heap memory, used by this {@code ChronicleHash} (in bytes)
     */
    long offHeapMemoryUsed();

    /**
     * @return the class of {@code <K>}
     */
    Class<K> keyClass();

    /**
     * Returns a context to perform arbitrary operations with the given key in this store.
     * Conventionally, should be used in a try-with-resources block: <pre>{@code
     * try (ExternalHashQueryContext<K> q = hash.queryContext(key)) {
     *     // ... do something
     * }}</pre>
     * <p>
     * <p>See documentation to {@link HashQueryContext} interface and methods in {@link MapMethods}
     * interface for examples of using contexts. Also see <a href="
     * https://github.com/OpenHFT/Chronicle-Map#working-with-an-entry-within-a-context-section">
     * Working with an entry within a context</a> section in the Chronicle Map tutorial.
     *
     * @param key the queried key
     * @return the context to perform operations with the key
     * @see HashQueryContext
     * @see MapQueryContext
     * @see ExternalHashQueryContext
     * @see MapMethods
     */
    @NotNull
    EQC queryContext(K key);

    /**
     * Returns a context to perform arbitrary operations with the given key, provided in
     * {@link Data} form. Equivalent to {@link #queryContext(Object)}, but accepts {@code Data}
     * instead of object key. This method is useful, when you already have {@code Data}, calling
     * this method instead of {@link #queryContext(Object)} might help to avoid unnecessary
     * deserialization.
     * <p>
     * <p>See documentation to {@link HashQueryContext} interface and methods in {@link MapMethods}
     * interface for examples of using contexts. Also see <a href="
     * https://github.com/OpenHFT/Chronicle-Map#working-with-an-entry-within-a-context-section">
     * Working with an entry within a context</a> section in the Chronicle Map tutorial.
     *
     * @param key the queried key as {@code Data}
     * @return the context to perform operations with the key
     */
    @NotNull
    EQC queryContext(Data<K> key);

    /**
     * Returns a context to perform arbitrary operations with the given key, provided in the
     * serialized form. See {@link #queryContext(Object)} and {@link #queryContext(Data)} for more
     * information on contexts semantics and usage patterns.
     *
     * @param keyBytes the bytes store with the key bytes to query
     * @param offset   actual offset of the key bytes within the given BytesStore
     * @param size     length of the key bytes sequence within the given BytesStore
     * @return the context to perform operations with the key
     */
    @NotNull
    EQC queryContext(BytesStore keyBytes, long offset, long size);

    /**
     * Returns a context of the segment with the given index. Segments are indexed from 0 to
     * {@link #segments()}{@code - 1}.
     *
     * @see HashSegmentContext
     */
    SC segmentContext(int segmentIndex);

    /**
     * Returns the number of segments in this {@code ChronicleHash}.
     *
     * @see ChronicleHashBuilder#minSegments(int)
     * @see ChronicleHashBuilder#actualSegments(int)
     */
    int segments();

    /**
     * Checks the given predicate on each entry in this {@code ChronicleHash} until all entries
     * have been processed or the predicate returns {@code false} for some entry, or throws
     * an {@code Exception}. Exceptions thrown by the predicate are relayed to the caller.
     * <p>
     * <p>The order in which the entries will be processed is unspecified. It might differ from
     * the order of iteration via {@code Iterator} returned by any method of this
     * {@code ChronicleHash} or it's collection view.
     * <p>
     * <p>If the {@code ChronicleHash} is empty, this method returns {@code true} immediately.
     *
     * @param predicate the predicate to be checked for each entry
     * @return {@code true} if the predicate returned {@code true} for all entries of
     * the {@code ChronicleHash}, {@code false} if it returned {@code false} for the entry
     */
    boolean forEachEntryWhile(Predicate<? super E> predicate);

    /**
     * Performs the given action for each entry in this {@code ChronicleHash} until all entries have
     * been processed or the action throws an {@code Exception}. Exceptions thrown by the action are
     * relayed to the caller.
     * <p>
     * <p>The order in which the entries will be processed is unspecified. It might differ from
     * the order of iteration via {@code Iterator} returned by any method of this
     * {@code ChronicleHash} or it's collection view.
     *
     * @param action the action to be performed for each entry
     */
    void forEachEntry(Consumer<? super E> action);

    /**
     * Releases the off-heap memory, used by this hash container and resources, used by replication,
     * if any. However, if hash container (hence off-heap memory, used by it) is mapped to the file
     * and there are other instances mapping the same data on the server across JVMs, the memory
     * won't be actually freed on operating system level. I. e. this method call doesn't affect
     * other {@link ChronicleMap} or {@link ChronicleSet} instances mapping the same data.
     * <p>
     * <p>If you won't call this method, memory would be held at least until next garbage
     * collection. This could be a problem if, for example, you target rare garbage collections,
     * but load and drop {@code ChronicleHash}es regularly.
     * <p>
     * <p>After this method call, all methods, querying the {@code ChronicleHash}'s entries, {@link
     * #longSize()} and {@code size()}), throw {@link ChronicleHashClosedException}. {@link
     * #isOpen()} returns {@code false}, {@code close()} itself returns immediately without effects
     * (i. e. repetitive {@code close()}, even from concurrent threads, are safe).
     */
    @Override
    void close();

    /**
     * Tells whether or not this {@code ChronicleHash} (on-heap instance) is open.
     *
     * @return {@code true} is {@link #close()} is not yet called
     */
    boolean isOpen();
}
