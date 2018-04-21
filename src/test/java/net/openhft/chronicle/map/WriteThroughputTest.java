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
