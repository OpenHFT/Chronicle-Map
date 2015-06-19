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

import net.openhft.lang.io.Bytes;
import net.openhft.lang.model.Byteable;

import java.io.Serializable;

/**
 * Strategy of preparing bytes of off-heap memory before assigning them into {@link Byteable}
 * value during {@link ChronicleMap#acquireUsing(Object, Object)
 * chronicleMap.acquireUsing()} call, when the given key is absent in the map. Example: if you
 * want {@code 1L} to be the off-heap updatable ChronicleMap's {@linkplain
 * ChronicleMapBuilder#defaultValue(Object) default value}.
 * PrepareValueBytes) configure} custom {@code PrepareValueBytes} strategy in {@link
 * ChronicleMapBuilder}: <pre>{@code
 * ChronicleMapBuilder.of(Key.class, LongValue.class)
 *     .prepareDefaultValueBytes((bytes, k) -> bytes.writeLong(1L))
 *     ...
 *     .create();}</pre>
 *
 * <p>Although the value type is not used in the API, {@link PrepareValueBytes} is not intended
 * to be generic by value type, because strategy should know the value layout and
 * {@linkplain Byteable#maxSize() how much bytes} does it take in memory.
 *
 * @param <K> the key type of {@link ChronicleMap}, on which {@code acquireUsing()} is queried
 * @param <V> the value type of {@link ChronicleMap}, on which {@code acquireUsing()} is queried

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
