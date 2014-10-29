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

package net.openhft.chronicle.map.fromdocs;

import net.openhft.lang.model.constraints.MaxSize;

public interface BondVOInterface {
    /* add support for entry based locking */
    @Deprecated()
    void busyLockEntry() throws InterruptedException;

    @Deprecated()
    void unlockEntry();

    long getIssueDate();

    void setIssueDate(long issueDate);  /* time in millis */

    long getMaturityDate();

    void setMaturityDate(long maturityDate);  /* time in millis */

    long addAtomicMaturityDate(long toAdd);

    boolean compareAndSwapCoupon(double expected, double value);

    double getCoupon();

    void setCoupon(double coupon);

    double addAtomicCoupon(double toAdd);

    String getSymbol();

    void setSymbol(@MaxSize(20) String symbol);

    // OpenHFT Off-Heap array[ ] processing notice ‘At’ suffix
    void setMarketPxIntraDayHistoryAt(@MaxSize(7) int tradingDayHour, MarketPx mPx);

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
