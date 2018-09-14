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
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.hash.serialization.SizedReader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class ByteArraySizedReader
        implements SizedReader<byte[]>, EnumMarshallable<ByteArraySizedReader> {

    public static final ByteArraySizedReader INSTANCE = new ByteArraySizedReader();

    private ByteArraySizedReader() {
    }

    @NotNull
    @Override
    public byte[] read(@NotNull Bytes in, long size, @Nullable byte[] using) {
        if (size < 0L || size > (long) Integer.MAX_VALUE) {
            throw new IORuntimeException("byte[] size should be non-negative int, " +
                    size + " given. Memory corruption?");
        }
        int arrayLength = (int) size;
        if (using == null || arrayLength != using.length)
            using = new byte[arrayLength];
        in.read(using);
        return using;
    }

    @Override
    public ByteArraySizedReader readResolve() {
        return INSTANCE;
    }
}
