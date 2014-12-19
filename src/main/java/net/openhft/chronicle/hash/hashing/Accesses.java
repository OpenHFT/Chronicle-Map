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

package net.openhft.chronicle.hash.hashing;

import net.openhft.lang.io.Bytes;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Static methods returning useful {@link Access} implementations.
 */
public final class Accesses {

    /**
     * Returns the {@code Access} delegating {@code getXXX(input, offset)} methods to {@code
     * sun.misc.Unsafe.getXXX(input, offset)}.
     *
     * <p>Usage example: <pre>{@code
     * class Pair {
     *     long first, second;
     *
     *     static final long pairDataOffset =
     *         theUnsafe.objectFieldOffset(Pair.class.getDeclaredField("first"));
     *
     *     static long hashPair(Pair pair, LongHashFunction hashFunction) {
     *         return hashFunction.hash(pair, Accesses.unsafe(), pairDataOffset, 16L);
     *     }
     * }}</pre>
     *
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

    private Accesses() {}
}
