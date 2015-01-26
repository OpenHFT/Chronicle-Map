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

package net.openhft.chronicle.hash.serialization.internal;

import net.openhft.chronicle.hash.hashing.LongHashFunction;
import net.openhft.chronicle.hash.serialization.BytesInterop;
import net.openhft.lang.io.Bytes;
import org.jetbrains.annotations.NotNull;

public enum BytesBytesInterop implements BytesInterop<Bytes> {
    INSTANCE;

    @Override
    public boolean startsWith(@NotNull Bytes bytes, @NotNull Bytes e) {
        return bytes.compare(bytes.position(), e, e.position(), e.remaining());
    }

    @Override
    public boolean equivalent(@NotNull Bytes a, @NotNull Bytes b) {
        if (a.remaining() != b.remaining())
            return false;
        return a.compare(a.position(), b, b.position(), b.remaining());
    }

    @Override
    public long hash(@NotNull LongHashFunction hashFunction, @NotNull Bytes e) {
        return hashFunction.hashBytes(e);
    }

    @Override
    public long size(@NotNull Bytes e) {
        return e.remaining();
    }

    @Override
    public void write(@NotNull Bytes bytes, @NotNull Bytes e) {
        bytes.write(e);
    }
}
