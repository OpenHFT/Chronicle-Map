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

package net.openhft.lang.values;

import net.openhft.chronicle.bytes.Byteable;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.DynamicallySized;
import net.openhft.chronicle.bytes.NativeBytesStore;
import net.openhft.chronicle.values.Copyable;

/**
 * Created by peter.lawrey on 23/04/2015.
 */
public class DoubleArray implements Byteable, Copyable<DoubleArray>, DynamicallySized {
    static boolean HACK = true;
    private static int CAPACITY = 0; // assume a 32-bit size.
    private static int LENGTH = CAPACITY + 4; // assume a 32-bit size.
    private static int BASE = LENGTH + 4;

    private final int capacity;
    private BytesStore bs;
    private long offset;

    public DoubleArray(int capacity) {
        bs = NativeBytesStore.nativeStoreWithFixedCapacity(BASE + capacity * 8L);
        bs.writeInt(CAPACITY, capacity);
        offset = 0;
        this.capacity = capacity;
    }

    @Override
    public void bytesStore(BytesStore bytes, long offset, long maxSize) {
        this.bs = bytes;
        this.offset = offset;
    }

    @Override
    public BytesStore bytesStore() {
        return bs;
    }

    @Override
    public long offset() {
        return offset;
    }

    @Override
    public long maxSize() {
        return BASE + capacity * 8;
    }

    public int length() {
        return HACK && bs == null ? 6 * 8 : bs.readInt(LENGTH + offset);
    }

    public int capacity() {
        return bs.readInt(CAPACITY + offset);
    }

    public double getDataAt(int index) {
        if (index < 0 || index >= length()) throw new ArrayIndexOutOfBoundsException();
        return bs.readDouble(BASE + offset + index * 8L);
    }

    public void setDataAt(int index, double d) {
        if (index < 0 || index >= capacity()) throw new ArrayIndexOutOfBoundsException();
        if (length() <= index)
            setLength(index + 1);
        bs.writeDouble(BASE + offset + index * 8L, d);
    }

    public void setLength(int length) {
        if (length < 0 || length >= capacity()) throw new IllegalArgumentException();
        bs.writeInt(LENGTH + offset, length);
    }

    public void addData(double d) {
        int index = length();
        if (index >= capacity()) throw new IllegalStateException();
        bs.writeInt(LENGTH + offset, index + 1);
        bs.writeDouble(BASE + offset + index * 8L, d);
    }

    public void setData(double[] doubles) {
        if (doubles.length > capacity()) throw new IllegalArgumentException();
        bs.writeInt(LENGTH + offset, doubles.length);
        for (int index = 0; index < doubles.length; index++)
            bs.writeDouble(BASE + offset + index * 8L, doubles[index]);
    }

    public int getDataUsing(double[] doubles) {
        int length = Math.min(length(), doubles.length);
        for (int index = 0; index < length; index++)
            doubles[index] = bs.readDouble(BASE + offset + index * 8L);
        return length;
    }

    @Override
    public void copyFrom(DoubleArray doubleArray) {
        int length = length();
        // set first so we check the length will fit.
        doubleArray.setLength(length);
        for (int i = 0; i < length; i++)
            doubleArray.setDataAt(i, getDataAt(i));
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[ ");
        String sep = "";
        for (int i = 0, len = length(); i < len; i++) {
            sb.append(sep).append(getDataAt(i));
            sep = ", ";
        }
        return sb.append(" ]").toString();
    }
}
