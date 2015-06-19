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

import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshallable;
import net.openhft.lang.model.Copyable;

import static net.openhft.lang.Compare.calcLongHashCode;
import static net.openhft.lang.Compare.isEqual;

public class BondVOInterface$$Heap implements BondVOInterface, BytesMarshallable, Copyable<net.openhft.chronicle.map.fromdocs.BondVOInterface>  {
    private double _coupon;
    private long _issueDate;
    private long _maturityDate;
    private int _entry;
    private net.openhft.chronicle.map.fromdocs.BondVOInterface.MarketPx[] _marketPxIntraDayHistory = new net.openhft.chronicle.map.fromdocs.BondVOInterface.MarketPx[7];
    {
        for (int i = 0; i < _marketPxIntraDayHistory.length; i++)
            _marketPxIntraDayHistory[i] = new net.openhft.chronicle.map.fromdocs.BondVOInterface$MarketPx$$Heap();
    }    private java.lang.String _symbol;

    public void setCoupon(double $) {
        _coupon = $;
    }

    public double getCoupon() {
        return _coupon;
    }

    public synchronized double addAtomicCoupon(double $) {
        return _coupon += $;
    }

    public synchronized boolean compareAndSwapCoupon(double _1, double _2) {
        if (_coupon == _1) {
            _coupon = _2;
            return true;
        }
        return false;
    }
    public void setIssueDate(long $) {
        _issueDate = $;
    }

    public long getIssueDate() {
        return _issueDate;
    }

    public void setMaturityDate(long $) {
        _maturityDate = $;
    }

    public long getMaturityDate() {
        return _maturityDate;
    }

    public synchronized long addAtomicMaturityDate(long $) {
        return _maturityDate += $;
    }

    public void unlockEntry() {
        throw new UnsupportedOperationException();
    }    public void busyLockEntry() {
        throw new UnsupportedOperationException();
    }    public void setMarketPxIntraDayHistoryAt(int i, net.openhft.chronicle.map.fromdocs.BondVOInterface.MarketPx $) {
        if(i<0) throw new ArrayIndexOutOfBoundsException(i + " must be greater than 0");
        if(i>=7) throw new ArrayIndexOutOfBoundsException(i + " must be less than 7");
        _marketPxIntraDayHistory[i] = $;
    }

    public net.openhft.chronicle.map.fromdocs.BondVOInterface.MarketPx getMarketPxIntraDayHistoryAt(int i) {
        if(i<0) throw new ArrayIndexOutOfBoundsException(i + " must be greater than 0");
        if(i>=7) throw new ArrayIndexOutOfBoundsException(i + " must be less than 7");
        return _marketPxIntraDayHistory[i];
    }

    public void setSymbol(java.lang.String $) {
        _symbol = $;
    }

    public java.lang.String getSymbol() {
        return _symbol;
    }

    @SuppressWarnings("unchecked")
    public void copyFrom(net.openhft.chronicle.map.fromdocs.BondVOInterface from) {
        setCoupon(from.getCoupon());
        setIssueDate(from.getIssueDate());
        setMaturityDate(from.getMaturityDate());
        for (int i = 0; i < 7; i++){
            setMarketPxIntraDayHistoryAt(i, from.getMarketPxIntraDayHistoryAt(i));
        }
        setSymbol(from.getSymbol());
    }

    public void writeMarshallable(Bytes out) {
        out.writeDouble(getCoupon());
        out.writeLong(getIssueDate());
        out.writeLong(getMaturityDate());
        for (int i = 0; i < 7; i++){
            out.writeObject(getMarketPxIntraDayHistoryAt(i));
        }
        out.writeUTFΔ(getSymbol());
    }
    public void readMarshallable(Bytes in) {
        _coupon = in.readDouble();
        _issueDate = in.readLong();
        _maturityDate = in.readLong();
        _entry = in.readInt();
        for (int i = 0; i < 7; i++){
            _marketPxIntraDayHistory[i] = in.readObject(net.openhft.chronicle.map.fromdocs.BondVOInterface.MarketPx.class);
        }
        _symbol = in.readUTFΔ();
    }

    public long longHashCode_marketPxIntraDayHistory() {
        long hc = 0;
        for (int i = 0; i < 7; i++) {
            hc += calcLongHashCode(getMarketPxIntraDayHistoryAt(i));
        }
        return hc;
    }

    public int hashCode() {
        long lhc = longHashCode();
        return (int) ((lhc >>> 32) ^ lhc);
    }

    public long longHashCode() {
        return ((((calcLongHashCode(getCoupon())) * 10191 +
                calcLongHashCode(getIssueDate())) * 10191 +
                calcLongHashCode(getMaturityDate())) * 10191 +
                longHashCode_marketPxIntraDayHistory()) * 10191 +
                calcLongHashCode(getSymbol());
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BondVOInterface)) return false;
        BondVOInterface that = (BondVOInterface) o;

        if(!isEqual(getCoupon(), that.getCoupon())) return false;
        if(!isEqual(getIssueDate(), that.getIssueDate())) return false;
        if(!isEqual(getMaturityDate(), that.getMaturityDate())) return false;
        for (int i = 0; i <7; i++) {
            if(!isEqual(getMarketPxIntraDayHistoryAt(i), that.getMarketPxIntraDayHistoryAt(i))) return false;
        }
        if(!isEqual(getSymbol(), that.getSymbol())) return false;
        return true;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("BondVOInterface{ ");
        sb.append(" }");
        return sb.toString();
    }
}
