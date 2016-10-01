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

import org.junit.Assert;
import org.junit.Test;

import java.lang.ref.WeakReference;

public class MemoryLeakTest {

    @Test
    public void testChronicleMapCollected() {
        WeakReference<ChronicleMap<Integer, Integer>> ref = getMapRef(false);
        System.gc();
        Assert.assertNull(ref.get());
    }

    @Test
    public void testReplicatedChronicleMapCollected() {
        WeakReference<ChronicleMap<Integer, Integer>> ref = getMapRef(true);
        System.gc();
        Assert.assertNull(ref.get());
    }

    private static WeakReference<ChronicleMap<Integer, Integer>> getMapRef(boolean replicated) {
        ChronicleMapBuilder<Integer, Integer> builder = ChronicleMap
                .of(Integer.class, Integer.class);
        if (replicated)
            builder.replication((byte) 1);
        ChronicleMap<Integer, Integer> map = builder.entries(1).create();
        map.put(1, 1);
        return new WeakReference<>(map);
    }
}
