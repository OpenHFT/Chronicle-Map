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

package net.openhft.chronicle.hash;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ExternalMapQueryContext;
import net.openhft.chronicle.map.MapEntry;
import net.openhft.chronicle.set.ChronicleSet;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.File;
import java.io.Serializable;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * This interface defines common {@link ChronicleMap} and {@link ChronicleSet}, related to off-heap
 * memory management and file-mapping. Not usable by itself.
 */
public interface ChronicleHash<K, C extends KeyContext<K>, EQC extends ExternalHashQueryContext<K>>
        extends Closeable {
    /**
     * Returns the file this hash container mapped to, i. e. when it is created by
     * {@link ChronicleHashBuilder#create()} call, or {@code null} if it is purely in-memory,
     * i. e. if it is created by {@link ChronicleHashBuilder#create()} call.
     *
     * @return the file this {@link ChronicleMap} or {@link ChronicleSet} is mapped to,
     *         or {@code null} if it is not mapped to any file
     * @see ChronicleHashBuilder#createPersistedTo(java.io.File)
     */
    File file();

    long longSize();

    /**
     * @deprecated use {@link #queryContext} instead
     */
    @Deprecated
    C context(K key);

    /**
     * @return the class of {@code <K>}
     */
    Class<K> keyClass();

    @NotNull
    EQC queryContext(K key);

    @NotNull EQC queryContext(Value<K, ?> key);
    
    /**
     * Checks the given predicate on each entry in this {@code ChronicleHash} until all entries
     * have been processed or the predicate returns {@code false} for some entry, or throws
     * an {@code Exception}. Exceptions thrown by the predicate are relayed to the caller.
     *
     * <p>The order in which the entries will be processed is unspecified. It might differ from
     * the order of iteration via {@code Iterator} returned by any method of this
     * {@code ChronicleHash} or it's collection view.
     *
     * <p>If the {@code ChronicleHash} is empty, this method returns {@code true} immediately.
     *
     * @param predicate the predicate to be checked for each entry
     * @return {@code true} if the predicate returned {@code true} for all entries of
     * the {@code ChronicleHash}, {@code false} if it returned {@code false} for the entry
     */
    boolean forEachEntryWhile(Predicate<? super C> predicate);

    /**
     * Performs the given action for each entry in this {@code ChronicleHash} until all entries have
     * been processed or the action throws an {@code Exception}. Exceptions thrown by the action are
     * relayed to the caller.
     *
     * <p>The order in which the entries will be processed is unspecified. It might differ from
     * the order of iteration via {@code Iterator} returned by any method of this
     * {@code ChronicleHash} or it's collection view.
     *
     * @param action the action to be performed for each entry
     */
    void forEachEntry(Consumer<? super C> action); // TODO add substitution and deprecate this

    /**
     * Releases the off-heap memory, used by this hash container and resources, used by replication,
     * if any. However, if hash container (hence off-heap memory, used by it) is mapped to the file
     * and there are other instances mapping the same data on the server across JVMs, the memory
     * won't be actually freed on operation system level. I. e. this method call doesn't affect
     * other {@link ChronicleMap} or {@link ChronicleSet} instances mapping the same data.
     *
     * <p>If you won't call this method, memory would be held at least until next garbage
     * collection. This could be a problem if, for example, you target rare garbage collections,
     * but load and drop {@code ChronicleHash}es regularly.
     *
     * <p>
     * TODO what about commit guarantees, when ChronicleMap is used with memory-mapped files, if
     * {@code ChronicleMap}/{@code ChronicleSet} closed/not?
     *
     * <p>After this method call behaviour of <i>all</i> methods of {@code ChronicleMap}
     * or {@code ChronicleSet} is undefined. <i>Any</i> method call on the map might throw
     * <i>any</i> exception, or even JVM crash. Shortly speaking, don't call use map closing.
     */
    @Override
    void close();
}
