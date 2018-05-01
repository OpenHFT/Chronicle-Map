/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.map.fromdocs;

import net.openhft.chronicle.values.Array;
import net.openhft.chronicle.values.Group;
import net.openhft.chronicle.values.MaxUtf8Length;

public interface BondVOInterface {

    @Group(0)
    long getEntryLockState();

    void setEntryLockState(long entryLockState);

    @Group(1)
    long getIssueDate();

    void setIssueDate(long issueDate);  /* time in millis */

    @Group(1)
    long getMaturityDate();

    void setMaturityDate(long maturityDate);  /* time in millis */

    long addAtomicMaturityDate(long toAdd);

    @Group(1)
    boolean compareAndSwapCoupon(double expected, double value);

    double getCoupon();

    void setCoupon(double coupon);

    double addAtomicCoupon(double toAdd);

    @Group(1)
    String getSymbol();

    void setSymbol(@MaxUtf8Length(20) String symbol);

    // OpenHFT Off-Heap array[ ] processing notice ‘At’ suffix
    @Group(1)
    @Array(length = 7)
    void setMarketPxIntraDayHistoryAt(int tradingDayHour, MarketPx mPx);

    /* 7 Hours in the Trading Day:
     * index_0 = 9.30am,
     * index_1 = 10.30am,
     …,
     * index_6 = 4.30pm
     */

    MarketPx getMarketPxIntraDayHistoryAt(int tradingDayHour);

    /* nested interface - empowering an Off-Heap hierarchical “TIER of prices”
    as array[ ] value */
    interface MarketPx {
        double getCallPx();

        void setCallPx(double px);

        double getParPx();

        void setParPx(double px);

        double getMaturityPx();

        void setMaturityPx(double px);

        double getBidPx();

        void setBidPx(double px);

        double getAskPx();

        void setAskPx(double px);
    }
}
