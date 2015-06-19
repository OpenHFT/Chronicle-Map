/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.lang.values;

import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshallable;
import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.Copyable;

import static net.openhft.lang.Compare.calcLongHashCode;
import static net.openhft.lang.Compare.isEqual;

public class IntValue$$Native implements IntValue, BytesMarshallable, Byteable, Copyable<net.openhft.lang.values.IntValue> {
    private static final int VALUE = 0;

    private Bytes _bytes;
    private long _offset;

    public void setValue(int $) {
        _bytes.writeInt(_offset + VALUE, $);
    }

    public int getValue() {
        return _bytes.readInt(_offset + VALUE);
    }

    public void setOrderedValue(int $) {
        _bytes.writeOrderedInt(_offset + VALUE, $);
    }

    public int getVolatileValue() {
        return _bytes.readVolatileInt(_offset + VALUE);
    }

    public int addValue(int $) {
        return _bytes.addInt(_offset + VALUE, $);
    }

    public int addAtomicValue(int $) {
        return _bytes.addAtomicInt(_offset + VALUE, $);
    }

    public boolean compareAndSwapValue(int _1, int _2) {
        return _bytes.compareAndSwapInt(_offset + VALUE, _1, _2);
    }

    public boolean tryLockNanosValue(long nanos) {
        return _bytes.tryLockNanosInt(_offset + VALUE, nanos);
    }

    public boolean tryLockValue() {
        return _bytes.tryLockInt(_offset + VALUE);
    }

    @Deprecated()
    public void unlockValue() {
        _bytes.unlockInt(_offset + VALUE);
    }

    @Deprecated()
    public void busyLockValue() throws InterruptedException {
        _bytes.busyLockInt(_offset + VALUE);
    }

    @Override
    public void copyFrom(net.openhft.lang.values.IntValue from) {
        setValue(from.getValue());
    }

    @Override
    public void writeMarshallable(Bytes out) {
        out.writeInt(getValue());
    }

    @Override
    public void readMarshallable(Bytes in) {
        setValue(in.readInt());
    }

    @Override
    public void bytes(Bytes bytes, long offset) {
        this._bytes = bytes;
        this._offset = offset;
    }

    @Override
    public Bytes bytes() {
        return _bytes;
    }

    @Override
    public long offset() {
        return _offset;
    }

    @Override
    public int maxSize() {
        return 4;
    }

    public int hashCode() {
        long lhc = longHashCode();
        return (int) ((lhc >>> 32) ^ lhc);
    }

    public long longHashCode() {
        return calcLongHashCode(getValue());
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IntValue)) return false;
        IntValue that = (IntValue) o;

        if (!isEqual(getValue(), that.getValue())) return false;
        return true;
    }

    public String toString() {
        if (_bytes == null) return "bytes is null";
        StringBuilder sb = new StringBuilder();
        sb.append("IntValue{ ");
        sb.append("value= ").append(getValue());
        sb.append(" }");
        return sb.toString();
    }
}