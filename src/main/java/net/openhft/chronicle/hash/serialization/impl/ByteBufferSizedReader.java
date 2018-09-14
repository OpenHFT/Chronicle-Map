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

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.serialization.SizedReader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

public final class ByteBufferSizedReader
        implements SizedReader<ByteBuffer>, EnumMarshallable<ByteBufferSizedReader> {
    public static final ByteBufferSizedReader INSTANCE = new ByteBufferSizedReader();

    private ByteBufferSizedReader() {
    }

    @NotNull
    @Override
    public ByteBuffer read(@NotNull Bytes in, long size, @Nullable ByteBuffer using) {
        if (size < 0L || size > (long) Integer.MAX_VALUE)
            throw new IllegalArgumentException("ByteBuffer size should be non-negative int, " +
                    size + " given. Memory corruption?");
        int bufferCap = (int) size;
        if (using == null || using.capacity() < bufferCap) {
            using = ByteBuffer.allocate(bufferCap);
        } else {
            using.position(0);
            using.limit(bufferCap);
        }
        in.read(using);
        using.flip();
        return using;
    }

    @Override
    public ByteBufferSizedReader readResolve() {
        return INSTANCE;
    }
}
