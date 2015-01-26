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

package net.openhft.chronicle.hash.serialization;

import net.openhft.chronicle.hash.ChronicleHash;
import net.openhft.lang.io.Bytes;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;

/**
 * Serializer (object to {@link Bytes} writer) which 1) knows the length of serialized form
 * of any object before actual serialization 2) doesn't include that length in the serialized
 * form itself, assuming it will be passed by the {@link ChronicleHash} into
 * {@link BytesReader#read(Bytes, long)} deserialization method.
 *
 * <p>Implementation example: <pre><code>
 * class LongPair { long first, second; }
 *
 * enum LongPairArrayWriter implements BytesWriter&lt;LongPair[]&gt; {
 *     INSTANCE;
 *
 *     &#064;Override
 *     public long size(LongPair[] longPairs) {
 *         return longPairs.length * 16L;
 *     }
 *
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
    long size(@NotNull E e);

    /**
     * Serializes the given object to the given {@code bytes}, without writing the length of the
     * serialized form itself.
     *
     * <p>Implementation of this method should increment the {@code bytes}' {@linkplain
     * Bytes#position() position} by {@link #size(Object) size(e)}. The given object should
     * be written into these range between the initial {@code bytes}' position and the position
     * after this method call returns. Implementation of this method must not alter the
     * {@code bytes}' {@linkplain Bytes#limit() limit} and contents outside of the specified range.
     *
     * @param bytes the {@code Bytes} to write the given object to
     * @param e the object to serialize to the given bytes
     * @see BytesReader#read(Bytes, long)
     */
    void write(@NotNull Bytes bytes, @NotNull E e);
}
