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

package net.openhft.lang.values;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.wire.AbstractBytesMarshallable;

public class MovingAverageCompact extends AbstractBytesMarshallable {

    private double movingAverage;
    private double high;
    private double low;
    private double stdDev;

    public MovingAverageCompact(double movingAverage, double high, double low, double stdDev) {
        this.movingAverage = movingAverage;
        this.high = high;
        this.low = low;
        this.stdDev = stdDev;
    }

    @Override
    public void readMarshallable(BytesIn in) throws IllegalStateException {
        movingAverage = in.readDouble();
        high = in.readDouble();
        low = in.readDouble();
        stdDev = in.readDouble();
    }

    @Override
    public void writeMarshallable(BytesOut out) {
        out.writeDouble(movingAverage);
        out.writeDouble(high);
        out.writeDouble(low);
        out.writeDouble(stdDev);
    }

    public double getMovingAverage() {
        return movingAverage;
    }

    public void setMovingAverage(double movingAverage) {
        this.movingAverage = movingAverage;
    }

    public double getHigh() {
        return high;
    }

    public void setHigh(double high) {
        this.high = high;
    }

    public double getLow() {
        return low;
    }

    public void setLow(double low) {
        this.low = low;
    }

    public double getStdDev() {
        return stdDev;
    }

    public void setStdDev(double stdDev) {
        this.stdDev = stdDev;
    }
}
