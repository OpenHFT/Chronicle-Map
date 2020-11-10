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
