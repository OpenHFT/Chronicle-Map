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
