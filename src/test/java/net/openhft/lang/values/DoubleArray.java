/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.lang.values;

import net.openhft.chronicle.bytes.Byteable;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.DynamicallySized;
import net.openhft.chronicle.values.Copyable;

import java.nio.channels.FileLock;

/**
 * Created by peter.lawrey on 23/04/2015.
 */
@SuppressWarnings({"rawtypes", "unchecked", "deprecation", "removal"})
public class DoubleArray implements Byteable, Copyable<DoubleArray>, DynamicallySized {
    static boolean HACK = true;
    private static final int CAPACITY = 0; // assume a 32-bit size.
    private static final int LENGTH = CAPACITY + 4; // assume a 32-bit size.
    private static final int BASE = LENGTH + 4;

    private final int capacity;
    private BytesStore<?, ?> bs;
    private long offset;

    public DoubleArray(int capacity) {
        bs = BytesStore.nativeStoreWithFixedCapacity(BASE + capacity * 8L);
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
    public BytesStore<?, ?> bytesStore() {
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

    @Override
    public FileLock lock(boolean shared) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileLock tryLock(boolean shared) {
        throw new UnsupportedOperationException();
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
