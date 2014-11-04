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

import net.openhft.chronicle.hash.serialization.BytesInterop;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.Hasher;
import net.openhft.lang.io.Bytes;

public enum ByteArrayMarshaller implements BytesInterop<byte[]>, BytesReader<byte[]> {
    INSTANCE;

    @Override
    public long size(byte[] ba) {
        return ba.length;
    }

    @Override
    public boolean startsWith(Bytes bytes, byte[] ba) {
        long pos = bytes.position();
        for (int i = 0; i < ba.length; i++) {
            if (bytes.readByte(pos + i) != ba[i])
                return false;
        }
        return true;
    }

    @Override
    public long hash(byte[] ba) {
        return Hasher.hash(ba, ba.length);
    }

    @Override
    public void write(Bytes bytes, byte[] ba) {
        bytes.write(ba);
    }

    @Override
    public byte[] read(Bytes bytes, long size) {
        byte[] ba = new byte[(int) size];
        bytes.read(ba);
        return ba;
    }

    @Override
    public byte[] read(Bytes bytes, long size, byte[] ba) {
        if (ba == null || size != ba.length)
            ba = new byte[(int) size];
        bytes.read(ba);
        return ba;
    }
}
