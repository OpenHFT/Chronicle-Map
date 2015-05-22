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

import static java.lang.Double.doubleToLongBits;

public enum DoubleMarshaller implements BytesInterop<Double>, BytesReader<Double>, SizeMarshaller {
    INSTANCE;

    @Override
    public long size(@NotNull Double e) {
        return 8L;
    }

    @Override
    public int sizeEncodingSize(long size) {
        return 0;
    }

    @Override
    public long minEncodableSize() {
        return 8L;
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
        assert size == 8L;
        // do nothing
    }

    @Override
    public boolean startsWith(@NotNull Bytes bytes, @NotNull Double e) {
        return doubleToLongBits(e) == bytes.readLong(bytes.position());
    }

    @Override
    public boolean equivalent(@NotNull Double a, @NotNull Double b) {
        return doubleToLongBits(a) == doubleToLongBits(b);
    }

    @Override
    public long hash(@NotNull LongHashFunction hashFunction, @NotNull Double e) {
        return hashFunction.hashLong(doubleToLongBits(e));
    }

    @Override
    public void write(@NotNull Bytes bytes, @NotNull Double e) {
        bytes.writeLong(doubleToLongBits(e));
    }

    @Override
    public long readSize(Bytes bytes) {
        return 8L;
    }

    @NotNull
    @Override
    public Double read(@NotNull Bytes bytes, long size) {
        return Double.longBitsToDouble(bytes.readLong());
    }

    @NotNull
    @Override
    public Double read(@NotNull Bytes bytes, long size, Double toReuse) {
        return read(bytes, size);
    }
}
