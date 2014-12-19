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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

enum ByteBufferAccess implements Access<ByteBuffer> {
    INSTANCE;

    @Override
    public long getLong(ByteBuffer input, long offset) {
        return input.getLong((int) offset);
    }

    @Override
    public long getUnsignedInt(ByteBuffer input, long offset) {
        return Primitives.unsignedInt(getInt(input, offset));
    }

    @Override
    public int getInt(ByteBuffer input, long offset) {
        return input.getInt((int) offset);
    }

    @Override
    public int getUnsignedShort(ByteBuffer input, long offset) {
        return Primitives.unsignedShort(getShort(input, offset));
    }

    @Override
    public int getShort(ByteBuffer input, long offset) {
        return input.getShort((int) offset);
    }

    @Override
    public int getUnsignedByte(ByteBuffer input, long offset) {
        return Primitives.unsignedByte(getByte(input, offset));
    }

    @Override
    public int getByte(ByteBuffer input, long offset) {
        return input.get((int) offset);
    }

    @Override
    public ByteOrder byteOrder(ByteBuffer input) {
        return input.order();
    }
}
