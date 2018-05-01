/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.map.impl.stage.data;

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.core.OS;
import org.jetbrains.annotations.Nullable;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
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
    public byte readVolatileByte(long offset) throws BufferUnderflowException {
        return 0;
    }

    @Override
    public short readVolatileShort(long offset) throws BufferUnderflowException {
        return 0;
    }

    @Override
    public int readVolatileInt(long offset) throws BufferUnderflowException {
        return 0;
    }

    @Override
    public long readVolatileLong(long offset) throws BufferUnderflowException {
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
    public long addressForRead(long offset) throws UnsupportedOperationException {
        return offset;
    }

    @Override
    public boolean compareAndSwapInt(long offset, int expected, int value)
            throws BufferOverflowException, IllegalArgumentException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean compareAndSwapLong(long offset, long expected, long value)
            throws BufferOverflowException, IllegalArgumentException {
        throw new UnsupportedOperationException();
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