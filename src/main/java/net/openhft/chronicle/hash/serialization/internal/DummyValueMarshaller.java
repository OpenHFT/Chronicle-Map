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

package net.openhft.chronicle.hash.serialization.internal;

import net.openhft.chronicle.hash.hashing.LongHashFunction;
import net.openhft.chronicle.hash.serialization.BytesInterop;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import net.openhft.lang.io.Bytes;
import org.jetbrains.annotations.NotNull;

import static net.openhft.chronicle.hash.serialization.internal.DummyValue.DUMMY_VALUE;

public enum DummyValueMarshaller
        implements BytesInterop<DummyValue>, BytesReader<DummyValue>, SizeMarshaller {
    INSTANCE;

    @Override
    public boolean startsWith(@NotNull Bytes bytes, @NotNull DummyValue dummyValue) {
        return true;
    }

    @Override
    public boolean equivalent(@NotNull DummyValue a, @NotNull DummyValue b) {
        return true;
    }

    @Override
    public long hash(@NotNull LongHashFunction hashFunction, @NotNull DummyValue dummyValue) {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public DummyValue read(@NotNull Bytes bytes, long size) {
        return DUMMY_VALUE;
    }

    @NotNull
    @Override
    public DummyValue read(@NotNull Bytes bytes, long size, DummyValue toReuse) {
        return DUMMY_VALUE;
    }

    @Override
    public long size(@NotNull DummyValue dummyValue) {
        return 0L;
    }

    @Override
    public void write(@NotNull Bytes bytes, @NotNull DummyValue dummyValue) {
        // do nothing
    }

    @Override
    public int sizeEncodingSize(long size) {
        return 0;
    }

    @Override
    public long minEncodableSize() {
        return 0L;
    }

    @Override
    public int minSizeEncodingSize() {
        return 0;
    }

    @Override
    public int maxSizeEncodingSize() {
        return 0;
    }

    @Override
    public void writeSize(Bytes bytes, long size) {
        assert size == 0L;
        // do nothing
    }

    @Override
    public long readSize(Bytes bytes) {
        return 0L;
    }
}
