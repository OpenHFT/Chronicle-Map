package net.openhft.lang.values;

import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.DirectStore;
import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.Copyable;

/**
 * Created by peter.lawrey on 23/04/2015.
 */
public class DoubleArray implements Byteable, Copyable<DoubleArray> {
    private static int LENGTH = 0; // assume a 32-bit size.
    private static int CAPACITY = LENGTH + 4; // assume a 32-bit size.
    private static int BASE = CAPACITY + 4;

    private Bytes bytes;
    private long offset;

    public DoubleArray(int capacity) {
        bytes = DirectStore.allocate(BASE + capacity * 8L).bytes();
        offset = 0;
    }


    @Override
    public void bytes(Bytes bytes, long offset) {
        this.bytes = bytes;
        this.offset = offset;
    }

    @Override
    public Bytes bytes() {
        return bytes;
    }

    @Override
    public long offset() {
        return offset;
    }

    @Override
    public int maxSize() {
        return BASE + length() * 8;
    }

    public int length() {
        return bytes.readInt(LENGTH + offset);
    }

    public int capacity() {
        return bytes.readInt(CAPACITY + offset);
    }

    public double getDataAt(int index) {
        if (index < 0 || index >= length()) throw new ArrayIndexOutOfBoundsException();
        return bytes.readDouble(BASE + offset + index * 8L);
    }

    public void setDataAt(int index, double d) {
        if (index < 0 || index >= capacity()) throw new ArrayIndexOutOfBoundsException();
        if (length() <= index)
            setLength(index + 1);
        bytes.writeDouble(BASE + offset + index * 8L, d);
    }

    public void setLength(int length) {
        if (length < 0 || length >= capacity()) throw new IllegalArgumentException();
        bytes.writeInt(LENGTH + offset, length);
    }

    public void addData(double d) {
        int index = length();
        if (index >= capacity()) throw new IllegalStateException();
        bytes.writeInt(LENGTH + offset, index + 1);
        bytes.writeDouble(BASE + offset + index * 8L, d);
    }

    public void setData(double[] doubles) {
        if (doubles.length > capacity()) throw new IllegalArgumentException();
        bytes.writeInt(LENGTH + offset, doubles.length);
        for (int index = 0; index < doubles.length; index++)
            bytes.writeDouble(BASE + offset + index * 8L, doubles[index]);
    }

    public int getDataUsing(double[] doubles) {
        int length = Math.min(length(), doubles.length);
        for (int index = 0; index < length; index++)
            doubles[index] = bytes.readDouble(BASE + offset + index * 8L);
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
}
