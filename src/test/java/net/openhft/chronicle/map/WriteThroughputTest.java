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

import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.stream.IntStream;

/**
 * Created by Peter Lawrey on 12/05/15.
 */
public class WriteThroughputTest {
    @Ignore("Long running")
    @Test
    public void bandwidthTest() throws IOException {
        int count = 2000;
        int size = 50 << 10;

        try (ChronicleMap<CharSequence, CharSequence> map = ChronicleMapBuilder
                .of(CharSequence.class, CharSequence.class)
                .entries(count * 3 / 2)
                .averageValueSize(size)
                .putReturnsNull(true)
                .create()) {

            System.out.println("ChronicleMap.put to memory");
            doTest(count, size, map);
        }

        File file = new File("bandwidthTest" + System.nanoTime() + ".deleteme");
        file.deleteOnExit();
        try (ChronicleMap<CharSequence, CharSequence> map = ChronicleMapBuilder
                .of(CharSequence.class, CharSequence.class)
                .entries(count * 3 / 2)
                .averageValueSize(size)
                .putReturnsNull(true)
                .createPersistedTo(file)) {

            System.out.println("ChronicleMap.put to disk");
            doTest(count, size, map);
        }

        File file2 = new File("bandwidthTest" + System.nanoTime() + ".snappy.deleteme");
        file2.deleteOnExit();
        try (ChronicleMap<CharSequence, CharSequence> map = ChronicleMapBuilder
                .of(CharSequence.class, CharSequence.class)
                .entries(count * 3 / 2)
                .averageValueSize(size / 4)
//                .valueReaderAndDataAccess(, SnappyStringMarshaller.INSTANCE, )
                .putReturnsNull(true)
                .createPersistedTo(file2)) {

            System.out.println("ChronicleMap.put to snappy disk");
            doTest(count, size, map);
        }
    }

    @Ignore("TODO")
    @Test
    public void bandwidthTestZ() throws IOException {
        int count = 2 << 10;
        int size = 50 << 10;

        File file3 = new File("bandwidthTest" + System.nanoTime() + ".Z.deleteme");
        file3.deleteOnExit();
        try (ChronicleMap<CharSequence, CharSequence> map = ChronicleMapBuilder
                .of(CharSequence.class, CharSequence.class)
                .entries(count * 3 / 2)
                .averageValueSize(size / 4)
                .valueMarshaller(DeflatorStringMarshaller.INSTANCE)
                .putReturnsNull(true)
                .createPersistedTo(file3)) {

            System.out.println("ChronicleMap.put to deflator disk");
            doTest(count, size, map);
        }
    }

    private void doTest(int count, int size, ChronicleMap<CharSequence, CharSequence> map) {
        StringBuilder value = new StringBuilder("value");
        while (value.length() < size)
            value.append(value.length());

        for (int t = 0; t < 1; t++) {
            StringBuilder key = new StringBuilder("key");
            long start = System.nanoTime();
            for (int i = 0; i < count; i++)
                map.put(key, value);
            long time = System.nanoTime() - start;
            System.out.printf("Serial %,d MB took %.3f seconds%n", size * count >> 20, time / 1e9);
        }

        for (int t = 0; t < 3; t++) {
            long start = System.nanoTime();
            IntStream.range(0, count).parallel().forEach(i -> {
                StringBuilder key = new StringBuilder("key");
                key.append(i);
                map.put(key, value);
            });
            long time = System.nanoTime() - start;
            System.out.printf("Concurrent %,d MB took %.3f seconds%n", size * count >> 20, time / 1e9);
        }
    }
}
