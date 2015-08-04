/*
 *      Copyright (C) 2015  higherfrequencytrading.com
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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.core.OS;

public enum ZeroRandomDataInput implements RandomDataInput {
    INSTANCE;

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
    public long realCapacity() {
        return Long.MAX_VALUE;
    }

    @Override
    public long capacity() {
        return Long.MAX_VALUE;
    }

    @Override
    public long address(long offset) throws UnsupportedOperationException {
        return offset;
    }

    @Override
    public Bytes bytesForRead() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Bytes bytesForWrite() {
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
}