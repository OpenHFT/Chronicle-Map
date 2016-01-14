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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.ChronicleHash;
import net.openhft.chronicle.wire.Marshallable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Deserializer of objects from bytes, mirroring the {@link SizedWriter}, i. e. assuming the length
 * of the serialized form isn't written in the beginning of the serialized form itself, but managed
 * by {@link ChronicleHash} implementation and passed to the reading methods.
 *
 * <p>Implementation example:<pre><code>
 * class LongPair { long first, second; }
 * 
 * enum LongPairArrayReader
 *         implements{@code SizedReader<LongPair[]>, EnumMarshallable<LongPairArrayReader>} {
 *     INSTANCE;
 *
 *    {@literal @}Override
 *    {@literal @}NotNull
 *     public LongPair[]{@literal read(@}NotNull Bytes in, long size,
 *            {@literal @}Nullable LongPair[] using) {
 *         if (size{@code >} Integer.MAX_VALUE * 16L)
 *             throw new IllegalStateException("LongPair[] size couldn't be " + (size / 16L));
 *         int resLen = (int) (size / 16L);
 *         LongPair[] res;
 *         if (using != null) {
 *             if (using.length == resLen) {
 *                 res = using;
 *             } else {
 *                 res = Arrays.copyOf(using, resLen);
 *             }
 *         } else {
 *             res = new LongPair[resLen];
 *         }
 *         for (int i = 0; i{@code <} resLen; i++) {
 *             LongPair pair = res[i];
 *             if (pair == null)
 *                 res[i] = pair = new LongPair();
 *             pair.first = in.readLong();
 *             pair.second = in.readLong();
 *         }
 *         return res;
 *     }
 *
 *    {@literal @}Override
 *     public LongPairArrayReader readResolve() {
 *         return INSTANCE;
 *     }
 * }</code></pre>
 *
 * @param <T> the type of the object deserialized
 * @see SizedWriter
 */
public interface SizedReader<T> extends Marshallable {

    /**
     * Reads and returns the object from {@link Bytes#readPosition()} (i. e. the current position)
     * to {@code Bytes.readPosition() + size} in the given {@code in}. Should attempt to reuse the
     * given {@code using} object, i. e. to read the deserialized data into the given object. If it
     * is possible, this objects then returned from this method. If it is impossible for any reason,
     * a new object should be created and returned. The given {@code using} object could be {@code
     * null}, in this case this method, of cause, should create a new object.
     *
     * <p>This method should increment the position in the given {@code Bytes} by the given {@code
     * size}.
     *
     * @param in the {@code Bytes} to read the object from
     * @param size the size of the serialized form of the returned object
     * @param using the object to read the deserialized data into, could be {@code null}
     * @return the object read from the bytes, either reused or newly created
     */
    @NotNull
    T read(Bytes in, long size, @Nullable T using);
}
