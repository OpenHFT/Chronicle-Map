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

package net.openhft.chronicle.map;

/**
 * Created by peter.lawrey on 28/02/14.
 */
public class Histogram {
    static final String[] FRACTION_STR = "worst,99.99,99.9,99,90,50".split(",");
    static final int[] FRACTIONS = {Integer.MAX_VALUE, 10000, 1000, 100, 10, 2};
    static final int BITS_OF_ACCURACY = 5;
    static final int MANTISSA = 52;
    static final int BITS_TO_TRUNCATE = MANTISSA - BITS_OF_ACCURACY;
    final int[] counters = new int[40 << BITS_OF_ACCURACY];
    long count = 0;

    public void sample(long value) {
        long rawValue = Double.doubleToRawLongBits(value) >> BITS_TO_TRUNCATE;
        int bucket = (int) rawValue - (1023 << BITS_OF_ACCURACY);
        counters[bucket]++;
        count++;
    }

    public void printResults() {
        for (int i = 0; i < counters.length; i++) {
            if (counters[i] == 0) continue;
            double d = indexToDouble(i);
            if (d < 10000 && d * counters[i] < 20000) continue;
            System.out.println((long) d + ":" + counters[i]);
        }
    }

    private double indexToDouble(int i) {
        return Double.longBitsToDouble((i + (1023L << BITS_OF_ACCURACY)) << BITS_TO_TRUNCATE);
    }

    public void printPercentiles() {
        printPercentiles("");
    }

    public void printPercentiles(String tail) {
        long[] times = new long[FRACTIONS.length];
        int f = 0;
        long total = 0;
        for (int pos = counters.length - 1; pos >= 0 && f < FRACTIONS.length; pos--) {
            total += counters[pos];
            if (total > count / FRACTIONS[f]) {
                times[f++] = (long) indexToDouble(pos);
            }
        }
        // print in reverse order.
        StringBuilder sb = new StringBuilder();
        for (int i = FRACTIONS.length - 1; i >= 0; i--) {
            sb.append(FRACTION_STR[i]).append("/");
        }
        sb.setLength(sb.length() - 1);
        sb.append(" : ");
        for (int i = FRACTIONS.length - 1; i >= 0; i--) {
            long time = times[i];
            if (time < 10000)
                sb.append(time / 100 / 10.0);
            else
                sb.append(time / 1000);
            sb.append(" / ");
        }
        sb.setLength(sb.length() - 3);
        sb.append(tail);
        System.out.println(sb);
    }
}
