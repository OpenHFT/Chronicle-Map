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

package net.openhft.chronicle.hash.hashing;

import net.openhft.lang.io.Bytes;

import java.nio.ByteBuffer;

/**
 * Static methods returning useful {@link Access} implementations.
 */
public final class Accesses {

    private Accesses() {
    }

    /**
     * Returns the {@code Access} delegating {@code getXXX(input, offset)} methods to {@code
     * sun.misc.Unsafe.getXXX(input, offset)}.
     * <p>
     * <p>Usage example: <pre>{@code
     * class Pair {
     *     long first, second;
     * <p>
     *     static final long pairDataOffset =
     *         theUnsafe.objectFieldOffset(Pair.class.getDeclaredField("first"));
     * <p>
     *     static long hashPair(Pair pair, LongHashFunction hashFunction) {
     *         return hashFunction.hash(pair, Accesses.unsafe(), pairDataOffset, 16L);
     *     }
     * }}</pre>
     * <p>
     * <p>{@code null} is a valid input, on accepting {@code null} {@code Unsafe} just interprets
     * the given offset as a wild memory address.
     *
     * @param <T> the type of objects to access
     * @return the unsafe memory {@code Access}
     */
    public static <T> Access<T> unsafe() {
        return (Access<T>) UnsafeAccess.INSTANCE;
    }

    /**
     * Returns the {@code Access} to any {@link ByteBuffer}.
     *
     * @return the {@code Access} to {@link ByteBuffer}s
     */
    public static Access<ByteBuffer> toByteBuffer() {
        return ByteBufferAccess.INSTANCE;
    }

    public static Access<Bytes> toBytes() {
        return BytesAccess.INSTANCE;
    }
}
