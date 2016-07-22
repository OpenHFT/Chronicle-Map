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

package net.openhft.chronicle.set;

import org.junit.Test;

import java.io.IOException;
import java.util.Random;

public class Issue3Test {

    @Test
    public void test() throws IOException {
        try (ChronicleSet<Long> set = ChronicleSetBuilder.of(Long.class)
                .actualSegments(1)
                .entriesPerSegment(1000)
                .create()) {
            Random r = new Random();
            for (int i = 0; i < 10; i++) {
                for (int j = 0; j < 300; j++) {
                    set.add(r.nextLong());
                }
                set.clear();
            }
        }
    }
}
