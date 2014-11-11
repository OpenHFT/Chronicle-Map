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

import net.openhft.chronicle.hash.serialization.BytesInterop;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.Hasher;
import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.values.LongValue;

public enum LongValueMarshaller
        implements BytesInterop<LongValue>, BytesReader<LongValue>, SizeMarshaller {
    INSTANCE;

    @Override
    public long size(LongValue e) {
        return 8L;
    }

    @Override
    public int sizeEncodingSize(long size) {
        return 0;
    }

    @Override
    public void writeSize(Bytes bytes, long size) {
        // do nothing
    }

    @Override
    public boolean startsWith(Bytes bytes, LongValue e) {
        return e.getValue() == bytes.readLong(0);
    }

    @Override
    public long hash(LongValue e) {
        return Hasher.hash(e.getValue());
    }

    @Override
    public void write(Bytes bytes, LongValue e) {
        bytes.writeLong(e.getValue());
    }

    @Override
    public long readSize(Bytes bytes) {
        return 8L;
    }

    @Override
    public LongValue read(Bytes bytes, long size) {
        return read(bytes, size, DataValueClasses.newInstance(LongValue.class));
    }

    @Override
    public LongValue read(Bytes bytes, long size, LongValue toReuse) {
        toReuse.setValue(bytes.readLong());
        return toReuse;
    }
}
