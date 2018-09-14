/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
