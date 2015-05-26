/*
 * Copyright 2015 Higher Frequency Trading
 *
 *  http://www.higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package net.openhft.chronicle.hash.impl;

import net.openhft.chronicle.bytes.Access;
import net.openhft.lang.io.Bytes;

import java.nio.ByteOrder;

enum JavaLangBytesAccess implements Access<Bytes> {
    INSTANCE;

    @Override
    public byte readByte(Bytes handle, long offset) {
        return handle.readByte(offset);
    }

    @Override
    public short readShort(Bytes handle, long offset) {
        return handle.readShort(offset);
    }

    @Override
    public char readChar(Bytes handle, long offset) {
        return handle.readChar(offset);
    }

    @Override
    public int readInt(Bytes handle, long offset) {
        return handle.readInt(offset);
    }

    @Override
    public long readLong(Bytes handle, long offset) {
        return handle.readLong(offset);
    }

    @Override
    public void writeByte(Bytes handle, long offset, byte i8) {
        handle.writeByte(offset, i8);
    }

    @Override
    public void writeShort(Bytes handle, long offset, short i) {
        handle.writeShort(offset, i);
    }

    @Override
    public void writeChar(Bytes handle, long offset, char c) {
        handle.writeChar(offset, c);
    }

    @Override
    public void writeInt(Bytes handle, long offset, int i) {
        handle.writeInt(offset, i);
    }

    @Override
    public void writeLong(Bytes handle, long offset, long i) {
        handle.writeLong(offset, i);
    }

    @Override
    public void writeFloat(Bytes handle, long offset, float d) {
        handle.writeFloat(offset, d);
    }

    @Override
    public void writeDouble(Bytes handle, long offset, double d) {
        handle.writeDouble(offset, d);
    }

    @Override
    public ByteOrder byteOrder(Bytes handle) {
        return handle.byteOrder();
    }
}
