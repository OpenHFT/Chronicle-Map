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

package net.openhft.lang.values;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DoubleArrayTest {

    @Test
    public void testSetData() {
        DoubleArray da = new DoubleArray(64);
        da.setData(new double[]{1, 2, 3, 4, 5, 6, 7, 8});
        assertEquals(8, da.length());
        for (int i = 0; i < da.length(); i++)
            assertEquals(i + 1.0, da.getDataAt(i), 0.0);
        da.addData(9);
        da.addData(10);
        double[] ds = new double[64];
        int len = da.getDataUsing(ds);
        assertEquals(10, len);
        for (int i = 0; i < len; i++)
            assertEquals(i + 1.0, ds[i], 0.0);

        for (int i = 0; i < 64; i++)
            ds[i] = i * 1.01;
        da.setData(ds);
        assertEquals(64, da.length());
        for (int i = 0; i < da.length(); i++)
            assertEquals(i * 1.01, da.getDataAt(i), 0.0);

        double[] ds2 = new double[65];
        assertEquals(64, da.getDataUsing(ds2));
        for (int i = 0; i < 64; i++) {
            assertEquals(i * 1.01, da.getDataAt(i), 0.0);
            assertEquals(i * 1.01, ds2[i], 0.0);
        }

        try {
            da.setData(ds2);
            fail();
        } catch (IllegalArgumentException expected) {
            // expected
        }
        // free the memory.
        da.bytesStore().release();
    }

    @Test
    public void addToAMap() {
        DoubleArray a = new DoubleArray(10);
        a.setData(new double[]{1, 2, 3, 4, 5});

        DoubleArray b = new DoubleArray(10);
        b.setData(new double[]{5, 6, 7, 8, 9});

        ChronicleMap<Integer, DoubleArray> proxyMap = ChronicleMap
                .of(Integer.class, DoubleArray.class)
                .constantValueSizeBySample(a)
                .entries(2)
                .create();
        proxyMap.put(1, a);
        proxyMap.put(2, b);

        System.out.println(proxyMap.get(1));
        System.out.println(proxyMap.get(2));
        proxyMap.close();
    }

    @Test
    @Ignore("TODO What is HACK???")
    public void addToAMap2() {
        DoubleArray.HACK = false;
        DoubleArray a = new DoubleArray(10);
        a.setData(new double[]{1, 2, 3, 4, 5});

        DoubleArray b = new DoubleArray(10);
        b.setData(new double[]{5, 6, 7, 8, 9});

        ChronicleMap<Integer, DoubleArray> proxyMap = ChronicleMapBuilder
                .of(Integer.class, DoubleArray.class)
                .averageValueSize(6 * 8)
                .create();
        proxyMap.put(1, a);
        proxyMap.put(2, b);

        System.out.println(proxyMap.get(1));
        System.out.println(proxyMap.get(2));
        proxyMap.close();
        DoubleArray.HACK = true;
    }
}