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

import net.openhft.chronicle.core.values.IntValue;
import net.openhft.lang.model.DataValueClasses;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertNotNull;

public class TrickyContextCasesTest {

    @Test(expected = IllegalStateException.class)
    public void nestedContextsSameKeyTest() {
        ChronicleMap<Integer, IntValue> map = ChronicleMapBuilder
                .of(Integer.class, IntValue.class)
                .entries(1).create();

        IntValue v = DataValueClasses.newInstance(IntValue.class);
        v.setValue(2);
        map.put(1, v);
        try (ExternalMapQueryContext<Integer, IntValue, ?> q = map.queryContext(1)) {
            q.writeLock().lock();
            // assume the value is 2
            IntValue v2 = q.entry().value().get();
            // this call should throw ISE, as accessing the key 1 in a nested context, but if not...
            map.remove(1);
            v.setValue(3);
            map.put(2, v);
            // prints 3
            System.out.println(v2.getValue());
        }
    }

    @Test(expected = Exception.class)
    public void testPutShouldBeWriteLocked() throws ExecutionException, InterruptedException {
        ChronicleMap<Integer, byte[]> map = ChronicleMapBuilder
                .of(Integer.class, byte[].class)
                .averageValue(new byte[1])
                .entries(100).actualSegments(1).create();
        map.put(1, new byte[] {1});
        map.put(2, new byte[] {2});
        try (ExternalMapQueryContext<Integer, byte[], ?> q = map.queryContext(1)) {
            MapEntry<Integer, byte[]> entry = q.entry(); // acquires read lock implicitly
            assertNotNull(entry);
            Executors.newFixedThreadPool(1).submit(() -> {
                // this call should try to acquire write lock, that should lead to dead lock
                // but if not...
                // relocates the entry for the key 1 after the entry for 2, under update lock
                map.put(1, new byte[] {1, 2, 3, 4, 5});
                // puts the entry for 3 at the place of the entry for the key 1, under update lock
                map.put(3, new byte[] {3});
            }).get();
            // prints [3]
            System.out.println(Arrays.toString(entry.value().get()));
        }
    }

}
