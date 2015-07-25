/*
 *      Copyright (C) 2015  higherfrequencytrading.com
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
public class PageLatencyMain {
    public static final int PAGES = Integer.getInteger("pages", 1024 * 1024);
    public static final int PAGES_SIZE = 512; // longs
    public static volatile long b;

    public static void main(String... ignored) {
        long[] bytes = new long[PAGES * PAGES_SIZE];
        long maxTime = 0;
        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < PAGES; i++) {
                long start0 = System.nanoTime();
                b = bytes[i * PAGES_SIZE];
                if (b != 0) throw new AssertionError();
                long time = System.nanoTime() - start0;
                if (time > maxTime) maxTime = time;
                if (time > 1e5)
                    System.out.println("Page access time was " + time / 100000 / 10.0 + " ms");
            }
        }
        System.out.println("Longest page access time was " + maxTime / 10000 / 100.0 + " ms");
    }
}
