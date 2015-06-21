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

package net.openhft.chronicle.map.fromdocs;

import net.openhft.chronicle.map.fromdocs.BondVOInterface.MarketPx;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshallable;
import net.openhft.lang.model.Copyable;

import static net.openhft.lang.Compare.calcLongHashCode;
import static net.openhft.lang.Compare.isEqual;

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
        return isEqual(getParPx(), that.getParPx());
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("BondVOInterface.MarketPx{ ");
        sb.append(" }");
        return sb.toString();
    }
}