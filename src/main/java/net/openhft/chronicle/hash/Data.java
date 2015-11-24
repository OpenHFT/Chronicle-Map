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

package net.openhft.chronicle.hash;

import net.openhft.chronicle.algo.hashing.LongHashFunction;
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.bytes.RandomDataOutput;
import org.jetbrains.annotations.Nullable;

import static net.openhft.chronicle.algo.bytes.Access.checkedRandomDataInputAccess;

/**
 * Dual bytes/object access to keys/values/elements.
 * 
 * <p>Bytes access: {@link #bytes()} + {@link #offset()} + {@link #size()}.
 * 
 * <p>Object access: {@link #get()}. 
 * 
 * <p>In most cases, each particular value wraps either some object or some bytes. Object
 * is marshalled to bytes lazily on demand, and bytes are lazily deserialized to object,
 * accordingly. 
 *  
 * @param <V> type of the accessed objects
 */
public interface Data<V> {

    /**
     * Returns the accessor object to the {@code Data}'s bytes. For safety, this interface returns
     * read-only object, because this object could expose bytes source that must be immutable, e. g.
     * an array behind {@code String} object.
     */
    RandomDataInput bytes();

    /**
     * Returns the offset to the {@code Data}'s bytes sequence, within the {@link #bytes()} object.
     * For example, the first byte of the bytes representation of this {@code Data} instance is
     * {@code data.bytes().readByte(data.offset())}.
     */
    long offset();

    /**
     * Returns the size of this {@code Data}'s bytes sequence. It spans from {@link #offset()} to
     * {@code offset() + size() - 1} bytes within the {@link #bytes()} object.
     */
    long size();

    /**
     * Computes hash code on the bytes representation of this {@code Data}, using the given hash
     * function.
     *
     * @param f the hash function to compute hash code using
     * @return the hash code of this {@code Data}'s bytes, computed by the given function
     */
    default long hash(LongHashFunction f) {
        return f.hash(bytes(), checkedRandomDataInputAccess(), offset(), size());
    }

    /**
     * Compares bytes of this {@code Data} to the given bytes {@code source}, starting from the
     * given offset.
     *
     * <p>Default implementation compares {@link #bytes()} of this {@code Data}, but custom
     * implementation may only check if {@linkplain #get() object} of this {@code Data} <i>would</i>
     * be serialized to the same bytes sequence, if this {@code Data} is object-based and obtaining
     * {@link #bytes()} requires serialization internally.
     *
     * @param source the bytes source, to compare this {@code Data}'s bytes with
     * @param sourceOffset the offset in the bytes source, the bytes sequence starts from
     * @return {@code true} if the given bytes sequence is equivalent to this {@code Data}'s bytes,
     * byte-by-byte
     */
    default boolean equivalent(RandomDataInput source, long sourceOffset) {
        return BytesUtil.bytesEqual(source, sourceOffset, bytes(), offset(), size());
    }

    /**
     * Writes bytes of this {@code Data} to the given {@code target} from the given {@code
     * targetOffset}.
     *
     * <p>Default implementation copies {@link #bytes()} of this {@code Data} using standard IO
     * methods of {@code RandomDataInput} and {@code RandomDataOutput}, but custom implementation
     * may write directly from {@linkplain #get() object}, if this {@code Data} is object-based and
     * obtaining {@link #bytes()} requires serialization internally. This allows to avoid double
     * copy.
     *
     * @param target the destination to write this data bytes to
     * @param targetOffset the offset in the target, to write the bytes from.
     */
    default void writeTo(RandomDataOutput target, long targetOffset) {
        target.write(targetOffset, bytes(), offset(), size());
    }

    /**
     * Returns "cached" object, generally not eligible for using outside some context, or a block,
     * synchronized with locks, or lambda, etc.
     * 
     * <p>If the {@code Data} is object wrapper -- this method just returns this object.
     */
    V get();

    /**
     * Reads the object from the value's bytes, trying to reuse the given object (might be {@code
     * null}).
     */
    V getUsing(@Nullable V using);

    /**
     * Utility method, compares two {@code Data} instances represent equivalent bytes sequences:
     * by comparing {@linkplain #size() their sizes}, then calling {@link
     * #equivalent(RandomDataInput, long) d1.equivalent(d2.bytes(), d2.offset())}.
     *
     * @param d1 the first {@code Data} to compare
     * @param d2 the second {@code Data} to compare
     * @return if the given {@code Data} instances represent equivalent bytes sequences
     */
    static boolean bytesEquivalent(Data<?> d1, Data<?> d2) {
        return d1.size() == d2.size() && d1.equivalent(d2.bytes(), d2.offset());
    }

    /**
     * {@code Data} implementations should override {@link Object#hashCode()} with delegation to
     * this method. Computes value's hash code by applying a hash function to {@code Data}'s
     * <i>bytes</i> representation.
     */
    default int dataHashCode() {
        return (int) hash(LongHashFunction.city_1_1());
    }

    /**
     * {@code Data} implementations should override {@link Object#equals(Object)} with delegation to
     * this method. Compares {@code Data}s' <i>bytes</i> representations.
     */
    default boolean dataEquals(Object obj) {
        return obj != null &&
                obj instanceof Data &&
                Data.bytesEquivalent(this, (Data<?>) obj);
    }
}
