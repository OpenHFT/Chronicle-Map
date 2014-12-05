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

import static net.openhft.lang.Compare.*;
import java.io.IOException;
import net.openhft.chronicle.map.fromdocs.BondVOInterface.MarketPx;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshallable;
import net.openhft.lang.model.Copyable;

public class BondVOInterface$MarketPx$$Heap implements MarketPx, BytesMarshallable, Copyable<net.openhft.chronicle.map.fromdocs.BondVOInterface.MarketPx>  {
    private double _askPx;
    private double _bidPx;
    private double _callPx;
    private double _maturityPx;
    private double _parPx;

    public void setAskPx(double $) {
        _askPx = $;
    }

    public double getAskPx() {
        return _askPx;
    }

    public void setBidPx(double $) {
        _bidPx = $;
    }

    public double getBidPx() {
        return _bidPx;
    }

    public void setCallPx(double $) {
        _callPx = $;
    }

    public double getCallPx() {
        return _callPx;
    }

    public void setMaturityPx(double $) {
        _maturityPx = $;
    }

    public double getMaturityPx() {
        return _maturityPx;
    }

    public void setParPx(double $) {
        _parPx = $;
    }

    public double getParPx() {
        return _parPx;
    }

    @SuppressWarnings("unchecked")
    public void copyFrom(net.openhft.chronicle.map.fromdocs.BondVOInterface.MarketPx from) {
        setAskPx(from.getAskPx());
        setBidPx(from.getBidPx());
        setCallPx(from.getCallPx());
        setMaturityPx(from.getMaturityPx());
        setParPx(from.getParPx());
    }

    public void writeMarshallable(Bytes out) {
        out.writeDouble(getAskPx());
        out.writeDouble(getBidPx());
        out.writeDouble(getCallPx());
        out.writeDouble(getMaturityPx());
        out.writeDouble(getParPx());
    }
    public void readMarshallable(Bytes in) {
        _askPx = in.readDouble();
        _bidPx = in.readDouble();
        _callPx = in.readDouble();
        _maturityPx = in.readDouble();
        _parPx = in.readDouble();
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
        StringBuilder sb = new StringBuilder();
        sb.append("BondVOInterface.MarketPx{ ");
        sb.append(" }");
        return sb.toString();
    }
}