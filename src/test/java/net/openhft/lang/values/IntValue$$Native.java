/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
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