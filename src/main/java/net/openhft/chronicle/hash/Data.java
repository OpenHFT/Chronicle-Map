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

import net.openhft.chronicle.algo.hashing.LongHashFunction;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.bytes.RandomDataOutput;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.MapEntry;
import net.openhft.chronicle.map.MapQueryContext;
import net.openhft.chronicle.set.ChronicleSet;
import org.jetbrains.annotations.Nullable;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Objects;

import static net.openhft.chronicle.algo.bytes.Access.checkedRandomDataInputAccess;

/**
 * Dual bytes/object access to keys or values (for {@link ChronicleMap}) and elements (for {@link
 * ChronicleSet}) throughout the Chronicle Map library.
 * <p>
 * <p>Bytes access: {@link #bytes()} + {@link #offset()} + {@link #size()}.
 * <p>
 * <p>Object access: {@link #get()}.
 * <p>
 * <p>In most cases, each particular {@code Data} wraps either some object or some bytes. Object
 * is marshalled to bytes lazily on demand, and bytes are lazily deserialized to object,
 * accordingly.
 *
 * @param <T> type of the accessed objects
 */
public interface Data<T> {

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
     * Returns the accessor object to the {@code Data}'s bytes.
     * <p>
     * <p>If this {@code Data} wraps some bytes, this method just returns a reference to that bytes.
     * <p>
     * <p>If this {@code Data} wraps an object, this method performs serialization internally and
     * returns a reference to the output buffer, caching the result for subsequent calls of this
     * method.
     * <p>
     * <p>For safety, this interface returns read-only object, because it could expose bytes source
     * that must be immutable, e. g. a `char[]` array behind a {@code String}. But in cases when the
     * {@code Data} instance wraps off-heap bytes, e. g. {@link MapEntry#value()}, it is allowed to
     * cast the object, returned from this method, to {@link BytesStore}, and write into the
     * off-heap memory. You should only ensure that current context (e. g. {@link MapQueryContext})
     * is locked exclusively, in order to avoid data races.
     */
    RandomDataInput bytes();

    /**
     * Returns the offset to the {@code Data}'s bytes sequence within the {@link RandomDataInput},
     * returned from {@link #bytes()} method. For example, the first byte of the bytes
     * representation of some {@code data} is {@code data.bytes().readByte(data.offset())}.
     */
    long offset();

    /**
     * Returns the size of this {@code Data}'s bytes sequence. It spans from {@link #offset()} to
     * {@code offset() + size() - 1} bytes within the {@link RandomDataInput}, returned from
     * {@link #bytes()} method.
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
     * <p>
     * <p>Default implementation compares {@link #bytes()} of this {@code Data}, but custom
     * implementation may only check if {@linkplain #get() object} of this {@code Data} <i>would</i>
     * be serialized to the same bytes sequence, if this {@code Data} wraps an object and obtaining
     * {@link #bytes()} requires serialization internally.
     *
     * @param source       the bytes source, to compare this {@code Data}'s bytes with
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
     * <p>
     * <p>Default implementation copies {@link #bytes()} of this {@code Data} using standard IO
     * methods of {@code RandomDataInput} and {@code RandomDataOutput}, but custom implementation
     * may write directly from {@linkplain #get() object}, if this {@code Data} is object-based and
     * obtaining {@link #bytes()} requires serialization internally. This allows to avoid double
     * copy.
     *
     * @param target       the destination to write this data bytes to
     * @param targetOffset the offset in the target, to write the bytes from.
     */
    default void writeTo(RandomDataOutput target, long targetOffset) {
        target.write(targetOffset, Objects.requireNonNull(bytes()), offset(), size());
    }

    /**
     * Returns object view of this {@code Data}.
     * <p>
     * <p>If this {@code Data} wraps some object, this method just returns that object.
     * <p>
     * <p>If this {@code Data} wraps some bytes, this method performs deserialization internally
     * and returns the resulting on-heap object, caching it for subsequent calls of this method. The
     * returned object could be reused, therefore it is <i>generally disallowed</i> to use the
     * object, returned from this method, <i>outside</i> some context, or a block, synchronized with
     * locks, or lambda, etc., which provided the access to this {@code Data} instance.
     */
    T get();

    /**
     * Deserialize and return an object from the {@code Data}'s bytes, trying to reuse the given
     * {@code using} object (might be {@code null}). This method either returns the given {@code
     * using} object back (if reuse is possible) or creates a new object, rather than some
     * internally cached and reused one, therefore it is <i>always allowed</i> to use the object,
     * returned from this method, <i>outside</i> some context, or a block, synchronized with locks,
     * or lambda, etc., which provided the access to this {@code Data} instance.
     */
    T getUsing(@Nullable T using);

    /**
     * {@code Data} implementations should override {@link Object#hashCode()} with delegation to
     * this method. Computes {@code Data}'s hash code by applying a hash function to {@code Data}'s
     * <i>bytes</i> representation.
     */
    default int dataHashCode() {
        return (int) hash(LongHashFunction.xx_r39());
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

    /**
     * {@code Data} implementations should override {@link Object#toString()} with delegation to
     * this method. Delegates to {@code Data}'s <i>object</i> {@code toString()}, i. e. equivalent
     * to calling {@code get().toString()} on this {@code Data} instance with some fallback, if
     * {@code get()} method throws an exception.
     */
    default String dataToString() {
        try {
            return get().toString();
        } catch (Exception e) {
            StringBuilder sb = new StringBuilder();
            sb.append("failed to deserialize object from data with bytes: [");
            RandomDataInput bs = bytes();
            for (long off = offset(), lim = offset() + size(); off < lim; off++) {
                sb.append(bs.printable(off));
            }
            sb.append("], exception: ");
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            sb.append(exceptionAsString);
            return sb.toString();
        }
    }
}
