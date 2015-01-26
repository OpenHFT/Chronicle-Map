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

import net.openhft.chronicle.hash.hashing.LongHashFunction;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.model.Byteable;
import org.jetbrains.annotations.NotNull;

/**
 * Writer for objects, which themselves are bytes sequence in some sense:
 * {@link Byteable Byteables}, {@code byte[]} arrays, other primitive arrays, "flat" objects
 * (with primitives fields only), particularly boxed primitive types.
 *
 * <p>This interface is called "BytesInterop" because it allows to work with object as they
 * are already marshalled to {@code Bytes}: {@linkplain #startsWith(Bytes, Object) compare}
 * with other {@code Bytes}, i. e. interoperate objects and {@code Bytes}.
 *
 * @param <E> type of marshalled objects
 */
public interface BytesInterop<E> extends BytesWriter<E> {

    /**
     * Checks if the given {@code bytes} starts (from the {@code bytes}' {@linkplain
     * Bytes#position() position}) with the byte sequence the given object
     * {@linkplain #write(Bytes, Object) is serialized} to, without actual serialization.
     *
     * @param bytes the bytes to check if starts with the serialized form of the given object.
     *              {@code bytes}' is positioned at the first byte to compare. {@code bytes}' limit
     *              is unspecified. {@code bytes}' position and limit shouldn't be altered during
     *              this call.
     * @param e the object to serialize virtually and compare with the given {@code bytes}
     * @return if the given {@code bytes} starts with the given {@code e} object's serialized form
     */
    boolean startsWith(@NotNull Bytes bytes, @NotNull E e);

    /**
     * Returns {@code true} if the given objects are serialized to the same sequence of bytes by
     * {@link #write(Bytes, Object)} method, without actual serialization. Normally this method
     * should always return the same result as {@code a.equals(b)} call, unless some fields of the
     * serialized objects, accounted in {@code equals()} implementation, are ignored
     * on serialization.
     * 
     * @param a first object to compare
     * @param b second object to compare
     * @return if the given objects are serialized to the same sequence of bytes by this
     * {@code BytesInterop} strategy
     */
    boolean equivalent(@NotNull E a, @NotNull E b);

    /**
     * Returns hash code for the byte sequence the given object {@linkplain #write(Bytes, Object)
     * is serialized} to, without actual serialization. Formally speaking, implementation of this
     * method should be equivalent to <pre>{@code
     * public long hash(LongHashFunction hashFunction, E e) {
     *     long size = size(e);
     *     long addr = theUnsafe.allocateMemory(size);
     *     Bytes bytes = new NativeBytes(addr, size);
     *     write(bytes, e);
     *     return hashFunction.hashBytes(bytes, 0, size);
     * }}</pre> except that it should perform actual serialization and allocate memory, of cause.
     *
     * @param hashFunction the hash function to use for hash code computation
     * @param e the object to hash
     * @return hash code for the bytes sequence the given object is serialized to
     */
    long hash(@NotNull LongHashFunction hashFunction, @NotNull E e);
}
