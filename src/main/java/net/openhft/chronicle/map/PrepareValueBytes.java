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

import net.openhft.lang.io.Bytes;
import net.openhft.lang.model.Byteable;

import java.io.Serializable;

/**
 * Strategy of preparing bytes of off-heap memory before assigning them into {@link Byteable}
 * value during {@link ChronicleMap#acquireUsing(Object, Object)
 * chronicleMap.acquireUsing()} call, when the given key is absent in the map. Example: if you
 * want {@code 1L} to be the off-heap updatable ChronicleMap's {@linkplain
 * OffHeapUpdatableChronicleMapBuilder#defaultValue(Object) default value}, the most simple way
 * to achieve this is to {@linkplain OffHeapUpdatableChronicleMapBuilder#prepareValueBytesOnAcquire(
 * PrepareValueBytes) configure} custom {@code PrepareValueBytes} strategy in {@link
 * OffHeapUpdatableChronicleMapBuilder}: <pre>{@code
 * OffHeapUpdatableChronicleMapBuilder.of(Key.class, LongValue.class)
 *     .prepareValueBytesOnAcquire((bytes, k) -> bytes.writeLong(1L))
 *     ...
 *     .create();}</pre>
 *
 * <p>Although the value type is not used in the API, {@link PrepareValueBytes} is not intended
 * to be generic by value type, because strategy should know the value layout and
 * {@linkplain Byteable#maxSize() how much bytes} does it take in memory.
 *
 * @param <K> the key type of {@link ChronicleMap}, on which {@code acquireUsing()} is queried
 * @param <V> the value type of {@link ChronicleMap}, on which {@code acquireUsing()} is queried
 * @see OffHeapUpdatableChronicleMapBuilder#prepareValueBytesOnAcquire(PrepareValueBytes)
 */
public interface PrepareValueBytes<K, V> extends Serializable {

    /**
     * Prepares bytes before assigning them into {@link Byteable}. The given bytes' position
     * corresponds to the start of value bytes to be assigned into. This method should increment
     * the given bytes' position by {@linkplain Byteable#maxSize() the value size}. Bytes' contents
     * outside of this range shouldn't be altered.
     *
     * @param bytes the bytes, positioned at the first value byte. Bytes' {@linkplain Bytes#limit(
     * long) limit} and {@linkplain Bytes#remaining()} are not specified, but it is guaranteed that
     * the latter is not less than {@linkplain Byteable#maxSize() the value size}.
     * @param key the key which is absent in the {@link ChronicleMap} during {@link
     * ChronicleMap#acquireUsing(Object, Object) acquireUsing()} call and value bytes are prepared
     * for
     */
    void prepare(Bytes bytes, K key);
}
