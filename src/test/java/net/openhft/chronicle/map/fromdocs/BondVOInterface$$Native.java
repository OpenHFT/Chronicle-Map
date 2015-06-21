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
import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.Copyable;

import static net.openhft.lang.Compare.calcLongHashCode;
import static net.openhft.lang.Compare.isEqual;

public class BondVOInterface$$Native
        implements BondVOInterface, BytesMarshallable, Byteable,
        Copyable<BondVOInterface> {
    private static final int COUPON = 0;
    private static final int ISSUEDATE = 8;
    private static final int MATURITYDATE = 16;
    private static final int ENTRY = 24;
    private static final int MARKETPXINTRADAYHISTORY = 28;
    private final BondVOInterface$MarketPx$$Native _marketPxIntraDayHistory[] =
            new BondVOInterface$MarketPx$$Native[7];
    {
        for (int i = 0; i < 7; i++)
            _marketPxIntraDayHistory[i] = new BondVOInterface$MarketPx$$Native();
    }
    private static final int SYMBOL = 308;


    private Bytes _bytes;
    private long _offset;



    public void setCoupon(double $) {
        _bytes.writeDouble(_offset + COUPON, $);
    }

    public double getCoupon() {
        return _bytes.readDouble(_offset + COUPON);
    }

    public double addAtomicCoupon(double $) {
        return _bytes.addAtomicDouble(_offset + COUPON, $);
    }    public boolean compareAndSwapCoupon(double _1, double _2) {
        return _bytes.compareAndSwapDouble(_offset + COUPON, _1, _2);
    }

    public void setIssueDate(long $) {
        _bytes.writeLong(_offset + ISSUEDATE, $);
    }

    public long getIssueDate() {
        return _bytes.readLong(_offset + ISSUEDATE);
    }



    public void setMaturityDate(long $) {
        _bytes.writeLong(_offset + MATURITYDATE, $);
    }

    public long getMaturityDate() {
        return _bytes.readLong(_offset + MATURITYDATE);
    }

    public long addAtomicMaturityDate(long $) {
        return _bytes.addAtomicLong(_offset + MATURITYDATE, $);
    }    public void unlockEntry() {
        _bytes.unlockInt(_offset + ENTRY);
    }    public void busyLockEntry() throws InterruptedException {
        _bytes.busyLockInt(_offset + ENTRY);
    }    public void setMarketPxIntraDayHistoryAt(int i, BondVOInterface.MarketPx $) {
        _marketPxIntraDayHistory[i].copyFrom($);
    }

    public BondVOInterface.MarketPx getMarketPxIntraDayHistoryAt(int i) {
        return _marketPxIntraDayHistory[i];
    }



    public void setSymbol(java.lang.String $) {
        _bytes.writeUTFΔ(_offset + SYMBOL, 20, $);
    }

    public java.lang.String getSymbol() {
        return _bytes.readUTFΔ(_offset + SYMBOL);
    }

    @Override
    public void copyFrom(BondVOInterface from) {
        setCoupon(from.getCoupon());
        setIssueDate(from.getIssueDate());
        setMaturityDate(from.getMaturityDate());
        for (int i = 0; i < 7; i++){
            setMarketPxIntraDayHistoryAt(i, from.getMarketPxIntraDayHistoryAt(i));
        }
        setSymbol(from.getSymbol());
    }

    @Override
    public void writeMarshallable(Bytes out) {
        out.writeDouble(getCoupon());
        out.writeLong(getIssueDate());
        out.writeLong(getMaturityDate());
        for (int i = 0; i < 7; i++){
            _marketPxIntraDayHistory[i].writeMarshallable(out);
        }
        out.writeUTFΔ(getSymbol());
    }
    @Override
    public void readMarshallable(Bytes in) {
        setCoupon(in.readDouble());
        setIssueDate(in.readLong());
        setMaturityDate(in.readLong());
        for (int i = 0; i < 7; i++){
            _marketPxIntraDayHistory[i].readMarshallable(in);
        }
        setSymbol(in.readUTFΔ());
    }
    @Override
    public void bytes(Bytes bytes, long offset) {
        this._bytes = bytes;
        this._offset = offset;
        for (int i = 0; i < 7; i++){
            _marketPxIntraDayHistory[i].bytes(bytes,
                    _offset + MARKETPXINTRADAYHISTORY + (i * 40));
        }
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
        return 328;
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
            if(!isEqual(getMarketPxIntraDayHistoryAt(i), that.getMarketPxIntraDayHistoryAt(i)))
                return false;
        }
        return isEqual(getSymbol(), that.getSymbol());
    }

    public String toString() {
        if (_bytes == null) return "bytes is null";
        StringBuilder sb = new StringBuilder();
        sb.append("BondVOInterface{ ");
        sb.append(" }");
        return sb.toString();
    }
}
