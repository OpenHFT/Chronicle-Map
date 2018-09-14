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

package net.openhft.chronicle.map.impl.stage.data;

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.core.OS;
import org.jetbrains.annotations.Nullable;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

public enum ZeroBytesStore implements BytesStore<ZeroBytesStore, Void> {
    INSTANCE;

    @Override
    public long addressForWrite(long offset) throws UnsupportedOperationException, BufferOverflowException {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public int peekUnsignedByte(long offset) {
        return 0;
    }

    @Override
    public byte readVolatileByte(long offset) {
        return 0;
    }

    @Override
    public short readVolatileShort(long offset) {
        return 0;
    }

    @Override
    public int readVolatileInt(long offset) {
        return 0;
    }

    @Override
    public long readVolatileLong(long offset) {
        return 0;
    }

    @Override
    public boolean isDirectMemory() {
        return false;
    }

    @Override
    public byte readByte(long offset) {
        return 0;
    }

    @Override
    public short readShort(long offset) {
        return 0;
    }

    @Override
    public int readInt(long offset) {
        return 0;
    }

    @Override
    public long readLong(long offset) {
        return 0;
    }

    @Override
    public float readFloat(long offset) {
        return 0;
    }

    @Override
    public double readDouble(long offset) {
        return 0;
    }

    @Override
    public void nativeRead(long position, long address, long size) {
        OS.memory().setMemory(address, size, (byte) 0);
    }

    @Override
    public boolean sharedMemory() {
        return false;
    }

    @Override
    public ZeroBytesStore copy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long capacity() {
        return Long.MAX_VALUE;
    }

    @Nullable
    @Override
    public Void underlyingObject() {
        throw new UnsupportedOperationException();
    }

    @Override

    public void move(long from, long to, long length) {
        if (length != 0)
            throw new UnsupportedOperationException();
    }

    @Override
    public long addressForRead(long offset) {
        return offset;
    }

    @Override
    public boolean compareAndSwapInt(long offset, int expected, int value) {
        if (expected != 0 || value != 0)
            throw new UnsupportedOperationException();
        return true;
    }

    @Override
    public void testAndSetInt(long offset, int expected, int value) {
        if (expected != 0 || value != 0)
            throw new UnsupportedOperationException();
    }

    @Override
    public boolean compareAndSwapLong(long offset, long expected, long value) {
        if (expected != 0 || value != 0)
            throw new UnsupportedOperationException();
        return true;
    }


    @Override
    public void reserve() throws IllegalStateException {
    }

    @Override
    public void release() throws IllegalStateException {
    }

    @Override
    public long refCount() {
        return 0;
    }

    @Override
    public boolean tryReserve() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public ZeroBytesStore writeByte(long l, byte b) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ZeroBytesStore writeShort(long l, short i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ZeroBytesStore writeInt(long l, int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ZeroBytesStore writeOrderedInt(long l, int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ZeroBytesStore writeLong(long l, long l1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ZeroBytesStore writeOrderedLong(long l, long l1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ZeroBytesStore writeFloat(long l, float v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ZeroBytesStore writeDouble(long l, double v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ZeroBytesStore writeVolatileByte(long l, byte b) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ZeroBytesStore writeVolatileShort(long l, short i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ZeroBytesStore writeVolatileInt(long l, int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ZeroBytesStore writeVolatileLong(long l, long l1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ZeroBytesStore write(long l, byte[] bytes, int i, int i1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(long l, ByteBuffer byteBuffer, int i, int i1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ZeroBytesStore write(long l, RandomDataInput randomDataInput, long l1, long l2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void nativeWrite(long l, long l1, long l2) {
        throw new UnsupportedOperationException();
    }

}