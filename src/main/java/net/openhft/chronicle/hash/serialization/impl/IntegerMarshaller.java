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

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.hash.serialization.AgileBytesMarshaller;
import net.openhft.chronicle.hash.serialization.Hasher;
import net.openhft.lang.io.Bytes;

public enum IntegerMarshaller implements AgileBytesMarshaller<Integer> {
    INSTANCE;

    @Override
    public long size(Integer e) {
        return 4L;
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
    public boolean startsWith(Bytes bytes, Integer e) {
        return e == bytes.readInt(0);
    }

    @Override
    public long hash(Integer e) {
        return Hasher.hash(e);
    }

    @Override
    public void write(Bytes bytes, Integer e) {
        bytes.writeInt(e);
    }

    @Override
    public long readSize(Bytes bytes) {
        return 4L;
    }

    @Override
    public Integer read(Bytes bytes, long size) {
        return bytes.readInt();
    }

    @Override
    public Integer read(Bytes bytes, long size, Integer e) {
        return bytes.readInt();
    }
}
