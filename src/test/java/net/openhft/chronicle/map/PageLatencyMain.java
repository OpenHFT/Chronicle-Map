//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

package net.openhft.chronicle.map;

/**
 * Created by peter.lawrey on 28/02/14.
 */
public class PageLatencyMain {
    private static final int PAGES = Integer.getInteger("pages", 1024 * 1024);
    private static final int PAGES_SIZE = 512; // longs
    private static volatile long b;

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
