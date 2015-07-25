/*
 *      Copyright (C) 2015  higherfrequencytrading.com
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.hash.serialization;

import net.openhft.chronicle.hash.ChronicleHash;
import net.openhft.lang.io.Bytes;
import org.jetbrains.annotations.NotNull;
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
 *         if (size &gt; Integer.MAX_VALUE * 16L)
 *             throw new IllegalStateException("LongPair[] size couldn't be " + (size / 16L));
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
 *         for (int i = 0; i &lt; resLen; i++) {
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
    @NotNull
    E read(@NotNull Bytes bytes, long size);

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
    @NotNull
    E read(@NotNull Bytes bytes, long size, @Nullable E toReuse);
}
