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

import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.values.Values;

import java.io.File;
import java.io.IOException;

import static net.openhft.chronicle.algo.MemoryUnit.BYTES;
import static net.openhft.chronicle.algo.MemoryUnit.MEGABYTES;

public class VinceTest {
    public static void main(String[] args) throws IOException {
        long count = 100_000_000L;
        File f = File.createTempFile("vince", ".map");
        f.deleteOnExit();
        try (ChronicleMap<CharSequence, LongValue> catalog = ChronicleMap
                .of(CharSequence.class, LongValue.class)
                .entries(count)
                .averageKey("100000000")
                .putReturnsNull(true)
                .createPersistedTo(f)) {

            long prev = System.currentTimeMillis();

            StringBuilder key = new StringBuilder();
            LongValue value = Values.newHeapInstance(LongValue.class);

            for (long i = 1; i <= count; i++) {
                key.setLength(0);
                key.append(i);
                value.setValue(i);
                catalog.put(key, value);
                if ((i % 1_000_000) == 0) {
                    long now = System.currentTimeMillis();
                    System.out.printf("Average ns to insert per mi #%d: %d\n",
                            (i / 1_000_000), now - prev);
                    prev = now;
                }
            }
            System.out.println("file size " + MEGABYTES.convert(f.length(), BYTES) + " MB");
        }
    }
}
