package net.openhft.lang.values;

import java.io.Serializable;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.bytes.BytesOut;

public class MovingAverageCompact implements BytesMarshallable, Serializable{

	private static final long serialVersionUID = 1L;
	
	private double movingAverage;
	private double high;
	private double low;
	private double stdDev;
	
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

	public MovingAverageCompact(double movingAverage, double high, double low, double stdDev) {
		super();
		this.movingAverage = movingAverage;
		this.high = high;
		this.low = low;
		this.stdDev = stdDev;
	}
	
	public MovingAverageCompact()
	{
		this(0.0, 0.0, 0.0, 0.0);
	}
	
	

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(high);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(low);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(movingAverage);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(stdDev);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MovingAverageCompact other = (MovingAverageCompact) obj;
		if (Double.doubleToLongBits(high) != Double.doubleToLongBits(other.high))
			return false;
		if (Double.doubleToLongBits(low) != Double.doubleToLongBits(other.low))
			return false;
		if (Double.doubleToLongBits(movingAverage) != Double.doubleToLongBits(other.movingAverage))
			return false;
		if (Double.doubleToLongBits(stdDev) != Double.doubleToLongBits(other.stdDev))
			return false;
		return true;
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

	@Override
	public String toString() {
		return "MovingAverageCompact [" + movingAverage + ", " + high + ", " + low + ", " + stdDev + "]";
	}
	
	
}
