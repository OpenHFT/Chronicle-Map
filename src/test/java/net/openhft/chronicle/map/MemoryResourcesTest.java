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

import net.openhft.chronicle.bytes.NoBytesStore;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.values.IntValue;
import net.openhft.chronicle.values.Values;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class MemoryResourcesTest {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { false, false }, { false, true }, { true, false }, { true, true }
        });
    }

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private boolean replicated;
    private boolean persisted;

    public MemoryResourcesTest(boolean replicated, boolean persisted) {
        this.replicated = replicated;
        this.persisted = persisted;
    }

    @Before
    public void initNoBytesStore() {
        Assert.assertNotEquals(0, NoBytesStore.NO_PAGE);
    }

    @Test
    public void testChronicleMapCollectedAndDirectMemoryReleased()
            throws IOException, InterruptedException {
        if (OS.isLinux())
            return; // TODO enable this test when stable on Linux
        long nativeMemoryUsedBeforeMap = nativeMemoryUsed();
        WeakReference<ChronicleMap<IntValue, String>> ref = new WeakReference<>(getMap());
        Assert.assertNotNull(ref.get());
        long expectedNativeMemory = nativeMemoryUsedBeforeMap + ref.get().offHeapMemoryUsed();
        assertEquals(expectedNativeMemory, nativeMemoryUsed());
        // Wait until Map is collected by GC
        while (ref.get() != null) {
            System.gc();
            Thread.yield();
        }
        // Wait until Cleaner is called and memory is returned to the system
        for (int i = 0; nativeMemoryUsedBeforeMap != nativeMemoryUsed() && i < 60_000_000; i++) {
            System.gc();
            byte[] garbage = new byte[1_000_000];
            Thread.sleep(1);
        }
        Assert.assertEquals(nativeMemoryUsedBeforeMap, nativeMemoryUsed());
    }

    private long nativeMemoryUsed() {
        if (persisted) {
            return OS.memoryMapped();
        } else {
            return OS.memory().nativeMemoryUsed();
        }
    }

    @Test
    public void testExplicitChronicleMapCloseReleasesMemory() throws IOException {
        long nativeMemoryUsedBeforeMap = nativeMemoryUsed();
        try (ChronicleMap<IntValue, String> map = getMap()) {
            long expectedNativeMemory = nativeMemoryUsedBeforeMap + map.offHeapMemoryUsed();
            assertEquals(expectedNativeMemory, nativeMemoryUsed());
        }
        assertEquals(nativeMemoryUsedBeforeMap, nativeMemoryUsed());
    }

    private ChronicleMap<IntValue, String> getMap() throws IOException {
        ChronicleMapBuilder<IntValue, String> builder = ChronicleMap
                .of(IntValue.class, String.class);
        if (replicated)
            builder.replication((byte) 1);
        builder.entries(1).averageValueSize(1);
        VanillaChronicleMap<IntValue, String, ?> map;
        if (persisted) {
            map = (VanillaChronicleMap<IntValue, String, ?>)
                    builder.createPersistedTo(folder.newFile());
        } else {
            map = (VanillaChronicleMap<IntValue, String, ?>) builder.create();
        }
        IntValue key = Values.newHeapInstance(IntValue.class);
        int i = 0;
        while (!map.hasExtraTierBulks()) {
            key.setValue(i++);
            map.put(key, "string" + i);
        }
        return map;
    }
}
