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

import net.openhft.chronicle.core.values.IntValue;
import net.openhft.chronicle.values.Values;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static net.openhft.chronicle.algo.hashing.LongHashFunction.xx_r39;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TrickyContextCasesTest {

    @Test(expected = IllegalStateException.class)
    public void nestedContextsSameKeyTest() {
        ChronicleMap<Integer, IntValue> map = ChronicleMapBuilder
                .of(Integer.class, IntValue.class)
                .entries(1).create();

        IntValue v = Values.newHeapInstance(IntValue.class);
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
        map.put(1, new byte[]{1});
        map.put(2, new byte[]{2});
        try (ExternalMapQueryContext<Integer, byte[], ?> q = map.queryContext(1)) {
            MapEntry<Integer, byte[]> entry = q.entry(); // acquires read lock implicitly
            assertNotNull(entry);
            Executors.newFixedThreadPool(1).submit(() -> {
                // this call should try to acquire write lock, that should lead to dead lock
                // but if not...
                // relocates the entry for the key 1 after the entry for 2, under update lock
                map.put(1, new byte[]{1, 2, 3, 4, 5});
                // puts the entry for 3 at the place of the entry for the key 1, under update lock
                map.put(3, new byte[]{3});
            }).get();
            // prints [3]
            System.out.println(Arrays.toString(entry.value().get()));
        }
    }

    @Test
    public void testHashCollision() {
        try (ChronicleMap<ByteBuffer, Integer> map = ChronicleMap
                .of(ByteBuffer.class, Integer.class)
                .constantKeySizeBySample(ByteBuffer.allocate(128))
                .entries(2)
                .create()) {
            ByteBuffer key1 = ByteBuffer.allocate(128).order(LITTLE_ENDIAN);
            key1.putLong(0, 1);
            key1.putLong(32, 2);

            ByteBuffer key2 = ByteBuffer.allocate(128).order(LITTLE_ENDIAN);
            key2.putLong(0, 1 + 0xBA79078168D4BAFL);
            key2.putLong(32, 2 + 0x9C90005B80000000L);

            ByteBuffer key3 = ByteBuffer.allocate(128).order(LITTLE_ENDIAN);
            key3.putLong(0, 1 + 0xBA79078168D4BAFL * 2);
            key3.putLong(32, 2 + 0x9C90005B80000000L * 2);

            assertEquals(xx_r39().hashBytes(key1), xx_r39().hashBytes(key2));
            assertEquals(xx_r39().hashBytes(key1), xx_r39().hashBytes(key3));

            try (ExternalMapQueryContext<ByteBuffer, Integer, ?> c1 = map.queryContext(key1)) {
                c1.writeLock().lock();
                c1.insert(c1.absentEntry(), c1.wrapValueAsData(1));

                try (ExternalMapQueryContext<ByteBuffer, Integer, ?> c2 = map.queryContext(key2)) {
                    c2.writeLock().lock();
                    c2.insert(c2.absentEntry(), c2.wrapValueAsData(2));

                    c1.remove(c1.entry());

                    map.put(key3, 3);

                    c2.replaceValue(c2.entry(), c2.wrapValueAsData(22));
                }
            }

            assertEquals(2, map.size());
            assertEquals((Integer) 22, map.get(key2));
            assertEquals((Integer) 3, map.get(key3));
        }
    }
}
