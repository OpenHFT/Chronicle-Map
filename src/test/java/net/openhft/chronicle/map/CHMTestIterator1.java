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

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Anshul Shelley
 */

public class CHMTestIterator1 {
    public static void main(String[] args) throws Exception {
        AtomicLong alValue = new AtomicLong();
        AtomicLong alKey = new AtomicLong();
        int runs = 3000000;
        ChronicleMapBuilder<String, Long> builder = ChronicleMapBuilder.of(String.class, Long.class)

                .entries(runs);

        try (ChronicleMap<String, Long> chm = builder.create()) {
        /*chm.put("k1", alValue.incrementAndGet());
        chm.put("k2", alValue.incrementAndGet());
        chm.put("k3", alValue.incrementAndGet());
        chm.put("k4", alValue.incrementAndGet());
        chm.put("k5", alValue.incrementAndGet());*/
            //chm.keySet();

            for (int i = 0; i < runs; i++) {
                chm.put("k" + alKey.incrementAndGet(), alValue.incrementAndGet());
            }

            long start = System.nanoTime();
            for (Map.Entry<String, Long> entry : chm.entrySet()) {
                entry.getKey();
                entry.getValue();
            }
            long time = System.nanoTime() - start;
            System.out.println("Average iteration time was " + time / runs / 1e3 + "us, for " + runs / 1e6 + "m entries");
        }
    }

}