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

package net.openhft.chronicle.hash.serialization;

import net.openhft.chronicle.hash.ChronicleHash;
import net.openhft.lang.io.Bytes;

import java.io.Serializable;

/**
 * Serializer (object to {@link Bytes} writer) which 1) knows the length of serialized form
 * of any object before actual serialization 2) doesn't include that length in the serialized
 * form itself, assuming it will be passed by the {@link ChronicleHash} into
 * {@link BytesReader#read(Bytes, long)} deserialization method.
 * <p>
 * <p>Implementation example: <pre><code>
 * class LongPair { long first, second; }
 * <p>
 * enum LongPairArrayWriter implements BytesWriter&lt;LongPair[]&gt; {
 *     INSTANCE;
 * <p>
 *     &#064;Override
 *     public long size(LongPair[] longPairs) {
 *         return longPairs.length * 16L;
 *     }
 * <p>
 *     &#064;Override
 *     public void write(Bytes bytes, LongPair[] longPairs) {
 *         for (LongPair pair : longPairs) {
 *             bytes.writeLong(pair.first);
 *             bytes.writeLong(pair.second);
 *         }
 *     }
 * }</code></pre>
 *
 * @param <E> the type of the object marshalled
 * @see BytesReader
 * @see BytesInterop
 */
public interface BytesWriter<E> extends Serializable {

    /**
     * Returns the length (in bytes) of the serialized form of the given object. Serialization form
     * in terms of this interface, i. e. how much bytes are written to {@code bytes} on
     * {@link #write(Bytes, Object) write(bytes, e)} call.
     *
     * @param e the object which serialized form length should be returned
     * @return the length (in bytes) of the serialized form of the given object
     */
    long size(E e);

    /**
     * Serializes the given object to the given {@code bytes}, without writing the length of the
     * serialized form itself.
     * <p>
     * <p>Implementation of this method should increment the {@code bytes}' {@linkplain
     * Bytes#position() position} by {@link #size(Object) size(e)}. The given object should
     * be written into these range between the initial {@code bytes}' position and the position
     * after this method call returns. Implementation of this method must not alter the
     * {@code bytes}' {@linkplain Bytes#limit() limit} and contents outside of the specified range.
     *
     * @param bytes the {@code Bytes} to write the given object to
     * @param e     the object to serialize to the given bytes
     * @see BytesReader#read(Bytes, long)
     */
    void write(Bytes bytes, E e);
}
