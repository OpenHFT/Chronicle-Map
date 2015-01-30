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

import net.openhft.lang.io.Bytes;

import java.nio.ByteOrder;

public final class BytesAccess extends Access<Bytes> {
    public static final BytesAccess INSTANCE = new BytesAccess();
    
    private BytesAccess() {}

    @Override
    public long getLong(Bytes input, long offset) {
        return input.readLong(offset);
    }

    @Override
    public long getUnsignedInt(Bytes input, long offset) {
        return input.readUnsignedInt(offset);
    }

    @Override
    public int getInt(Bytes input, long offset) {
        return input.readInt(offset);
    }

    @Override
    public int getUnsignedShort(Bytes input, long offset) {
        return input.readUnsignedShort(offset);
    }

    @Override
    public int getShort(Bytes input, long offset) {
        return input.readShort(offset);
    }

    @Override
    public int getUnsignedByte(Bytes input, long offset) {
        return input.readUnsignedByte(offset);
    }

    @Override
    public int getByte(Bytes input, long offset) {
        return input.readByte(offset);
    }

    @Override
    public ByteOrder byteOrder(Bytes input) {
        return input.byteOrder();
    }
}
