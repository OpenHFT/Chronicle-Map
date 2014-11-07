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
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;

/**
 * Deserializer (object from {@link Bytes}, mirroring the {@link BytesWriter}, i. e. assuming the
 * length of the serialized form isn't written in the beginning of the serialized form itself,
 * but managed by {@link ChronicleHash} implementation and passed to the reading methods.
 *
 * <p>Implementation example:<pre><code>
 * class LongPair { long first, second; }
 * 
 * enum LongPairArrayReader implements BytesReader&lt;LongPair[]&gt; {
 *     INSTANCE;
 *
 *     &#064;Override
 *     public LongPair[] read(Bytes bytes, long size) {
 *         return read(bytes, size, null);
 *     }
 *
 *     &#064;Override
 *     public LongPair[] read(Bytes bytes, long size, LongPair[] toReuse) {
 *         int resLen = (int) (size / 16L);
 *         LongPair[] res;
 *         if (toReuse != null) {
 *             if (toReuse.length == resLen) {
 *                 res = toReuse;
 *             } else {
 *                 res = Arrays.copyOf(toReuse, resLen);
 *             }
 *         } else {
 *             res = new LongPair[resLen];
 *         }
 *         for (int i = 0; i < resLen; i++) {
 *             LongPair pair = res[i];
 *             if (pair == null)
 *                 res[i] = pair = new LongPair();
 *             pair.first = bytes.readLong();
 *             pair.second = bytes.readLong();
 *         }
 *         return res;
 *     }
 * }</code></pre>
 *
 * @param <E> the type of the object demarshalled
 * @see BytesWriter
 */
public interface BytesReader<E> extends Serializable {

    /**
     * Reads and returns the object from {@code [position(), position() + size]} bytes of the given
     * {@link Bytes}.
     *
     * <p>Implementation of this method should increment the {@code bytes}' position by the given
     * size, i. e. "consume" the bytes of the deserialized object. It must not alter the
     * {@code bytes}' {@link Bytes#limit() limit} and contents.
     *
     * @param bytes the {@code Bytes} to read the object from
     * @param size the size of the serialized form of the returned object
     * @return the object read from the bytes
     * @see BytesWriter#write(Bytes, Object)
     * @see #read(Bytes, long, Object)
     */
    E read(Bytes bytes, long size);

    /**
     * Similar to {@link #read(Bytes, long)}, but should attempt to reuse the given object, i. e.
     * to read the deserialized data into the given instance. If it is possible, this objects than
     * returned from this method. If it isn't possible for any reason, a new object should be
     * created and returned, just like in {@link #read(Bytes, long)} method. The given object could
     * be {@code null}, in this case this method should behave exactly the same as
     * {@link #read(Bytes, long)} method does.
     *
     * @param bytes the {@code Bytes} to read the object from
     * @param size the size of the serialized form of the returned object
     * @param toReuse the object to read the deserialized data into
     * @return the object read from the bytes, either reused or newly created
     * @see #read(Bytes, long)
     */
    E read(Bytes bytes, long size, @Nullable E toReuse);
}
