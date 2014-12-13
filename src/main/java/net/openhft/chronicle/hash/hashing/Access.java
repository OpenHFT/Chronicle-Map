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

import java.nio.ByteOrder;

/**
 * Strategy of reading bytes, defines the abstraction of {@code T} class instances as ordered byte
 * sequence. All {@code getXXX(input, offset)} should be consistent to each other in terms of
 * <i>ordered byte sequence</i> each {@code T} instance represents. For example, if some {@code
 * Access} implementation returns {@link ByteOrder#LITTLE_ENDIAN} on {@link #byteOrder(Object)
 * byteOrder(input)} call, the following expressions should always have the same value:
 * <ul>
 *     <li>{@code getLong(input, 0)}</li>
 *     <li>{@code getUnsignedInt(input, 0) | (getUnsignedInt(input, 4) << 32)}</li>
 *     <li><pre>{@code getUnsignedInt(input, 0) |
 *    ((long) getUnsignedShort(input, 4) << 32) |
 *    ((long) getUnsignedByte(input, 6) << 48) |
 *    ((long) getUnsignedByte(input, 7) << 56)}</pre></li>
 *   <li>And so on</li>
 * </ul>
 *
 * <p>{@code getXXX(input, offset)} methods could throw unchecked exceptions when requested bytes
 * range is outside of the bounds of the byte sequence, represented by the given {@code input}.
 * However, they could omit checks for better performance.
 *
 * <p>{@code Access} API is designed for inputs, that actually represent byte sequences that lay
 * continuously in memory. Theoretically {@code Access} strategy could be implemented for
 * non-continuous byte sequences, or abstractions which aren't actually present in memory as they
 * are accessed, but this should be awkward, and hashing using such {@code Access} is expected to
 * be slow.
 *
 * @param <T> the type of the object to access
 */
public interface Access<T> {

    /**
     * Reads {@code [offset, offset + 7]} bytes of the byte sequence represented by the given
     * {@code input} as a single {@code long} value.
     *
     * @param input the object to access
     * @param offset offset to the first byte to read within the byte sequence represented
     * by the given object
     * @return eight bytes as a {@code long} value, in {@linkplain #byteOrder(Object) the expected
     * order}
     */
    long getLong(T input, long offset);

    /**
     * Shortcut for {@code getInt(input, offset) & 0xFFFFFFFFL}. Could be implemented more
     * efficiently.
     *
     * @param input the object to access
     * @param offset offset to the first byte to read within the byte sequence represented
     * by the given object
     * @return four bytes as an unsigned int value, in {@linkplain #byteOrder(Object) the expected
     * order}
     */
    long getUnsignedInt(T input, long offset);

    /**
     * Reads {@code [offset, offset + 3]} bytes of the byte sequence represented by the given
     * {@code input} as a single {@code int} value.
     *
     * @param input the object to access
     * @param offset offset to the first byte to read within the byte sequence represented
     * by the given object
     * @return four bytes as an {@code int} value, in {@linkplain #byteOrder(Object) the expected
     * order}
     */
    int getInt(T input, long offset);

    /**
     * Shortcut for {@code getShort(input, offset) & 0xFFFF}. Could be implemented more
     * efficiently.
     *
     * @param input the object to access
     * @param offset offset to the first byte to read within the byte sequence represented
     * by the given object
     * @return two bytes as an unsigned short value, in {@linkplain #byteOrder(Object) the expected
     * order}
     */
    int getUnsignedShort(T input, long offset);

    /**
     * Reads {@code [offset, offset + 1]} bytes of the byte sequence represented by the given
     * {@code input} as a single {@code short} value, returned widened to {@code int}.
     *
     * @param input the object to access
     * @param offset offset to the first byte to read within the byte sequence represented
     * by the given object
     * @return two bytes as a {@code short} value, in {@linkplain #byteOrder(Object) the expected
     * order}, widened to {@code int}
     */
    int getShort(T input, long offset);

    /**
     * Shortcut for {@code getByte(input, offset) & 0xFF}. Could be implemented more efficiently.
     *
     * @param input the object to access
     * @param offset offset to the byte to read within the byte sequence represented
     * by the given object
     * @return a byte by the given {@code offset}, interpreted as unsigned
     */
    int getUnsignedByte(T input, long offset);

    /**
     * Reads a single byte at the given {@code offset} in the byte sequence represented by the given
     * {@code input}, returned widened to {@code int}.
     *
     * @param input the object to access
     * @param offset offset to the byte to read within the byte sequence represented
     * by the given object
     * @return a byte by the given {@code offset}, widened to {@code int}
     */
    int getByte(T input, long offset);

    /**
     * The byte order in which all multi-byte {@code getXXX()} reads from the given {@code input}
     * are performed.
     *
     * @param input the accessed object
     * @return the byte order of all multi-byte reads from the given {@code input}
     */
    ByteOrder byteOrder(T input);
}
