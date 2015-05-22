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

package net.openhft.chronicle.map.fromdocs;

import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshallable;
import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.Copyable;

import static net.openhft.lang.Compare.calcLongHashCode;
import static net.openhft.lang.Compare.isEqual;

public class BondVOInterface$MarketPx$$Native
        implements BondVOInterface.MarketPx, BytesMarshallable, Byteable,
        Copyable<BondVOInterface.MarketPx> {
    private static final int ASKPX = 0;
    private static final int BIDPX = 8;
    private static final int CALLPX = 16;
    private static final int MATURITYPX = 24;
    private static final int PARPX = 32;

    private Bytes _bytes;
    private long _offset;

    public void setAskPx(double $) {
        _bytes.writeDouble(_offset + ASKPX, $);
    }

    public double getAskPx() {
        return _bytes.readDouble(_offset + ASKPX);
    }

    public void setBidPx(double $) {
        _bytes.writeDouble(_offset + BIDPX, $);
    }

    public double getBidPx() {
        return _bytes.readDouble(_offset + BIDPX);
    }

    public void setCallPx(double $) {
        _bytes.writeDouble(_offset + CALLPX, $);
    }

    public double getCallPx() {
        return _bytes.readDouble(_offset + CALLPX);
    }

    public void setMaturityPx(double $) {
        _bytes.writeDouble(_offset + MATURITYPX, $);
    }

    public double getMaturityPx() {
        return _bytes.readDouble(_offset + MATURITYPX);
    }

    public void setParPx(double $) {
        _bytes.writeDouble(_offset + PARPX, $);
    }

    public double getParPx() {
        return _bytes.readDouble(_offset + PARPX);
    }

    @Override
    public void copyFrom(BondVOInterface.MarketPx from) {
        setAskPx(from.getAskPx());
        setBidPx(from.getBidPx());
        setCallPx(from.getCallPx());
        setMaturityPx(from.getMaturityPx());
        setParPx(from.getParPx());
    }

    @Override
    public void writeMarshallable(Bytes out) {
        out.writeDouble(getAskPx());
        out.writeDouble(getBidPx());
        out.writeDouble(getCallPx());
        out.writeDouble(getMaturityPx());
        out.writeDouble(getParPx());
    }
    @Override
    public void readMarshallable(Bytes in) {
        setAskPx(in.readDouble());
        setBidPx(in.readDouble());
        setCallPx(in.readDouble());
        setMaturityPx(in.readDouble());
        setParPx(in.readDouble());
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
        return 40;
    }
    public int hashCode() {
        long lhc = longHashCode();
        return (int) ((lhc >>> 32) ^ lhc);
    }

    public long longHashCode() {
        return ((((calcLongHashCode(getAskPx())) * 10191 +
                calcLongHashCode(getBidPx())) * 10191 +
                calcLongHashCode(getCallPx())) * 10191 +
                calcLongHashCode(getMaturityPx())) * 10191 +
                calcLongHashCode(getParPx());
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BondVOInterface.MarketPx)) return false;
        BondVOInterface.MarketPx that = (BondVOInterface.MarketPx) o;

        if(!isEqual(getAskPx(), that.getAskPx())) return false;
        if(!isEqual(getBidPx(), that.getBidPx())) return false;
        if(!isEqual(getCallPx(), that.getCallPx())) return false;
        if(!isEqual(getMaturityPx(), that.getMaturityPx())) return false;
        if(!isEqual(getParPx(), that.getParPx())) return false;
        return true;
    }

    public String toString() {
        if (_bytes == null) return "bytes is null";
        StringBuilder sb = new StringBuilder();
        sb.append("BondVOInterface.MarketPx{ ");
        sb.append(" }");
        return sb.toString();
    }
}
