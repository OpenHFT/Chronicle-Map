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
