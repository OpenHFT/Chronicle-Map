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

import com.google.common.collect.Lists;
import net.openhft.chronicle.bytes.NoBytesStore;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.values.IntValue;
import net.openhft.chronicle.hash.serialization.impl.StringSizedReader;
import net.openhft.chronicle.hash.serialization.impl.StringUtf8DataAccess;
import net.openhft.chronicle.map.impl.QueryContextInterface;
import net.openhft.chronicle.values.Values;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import sun.misc.Cleaner;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class MemoryLeaksTest {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        List<Boolean> booleans = Arrays.asList(false, true);
        // Test with all possible combinations of three boolean parameters.
        return Lists.cartesianProduct(booleans, booleans, booleans)
                .stream().map(List::toArray).collect(Collectors.toList());
    }

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private boolean persisted;
    private ChronicleMapBuilder<IntValue, String> builder;
    private boolean closeWithinContext;
    /**
     * Accounting {@link CountedStringReader} creation and finalization. All serializers,
     * created since the map creation, should become unreachable after map.close() or collection by
     * Cleaner, it means that map contexts (referencing serializers) are collected by the GC
     */
    private final AtomicInteger serializerCount = new AtomicInteger();
    private final List<WeakReference<CountedStringReader>> serializers = new ArrayList<>();

    public MemoryLeaksTest(boolean replicated, boolean persisted, boolean closeWithinContext) {
        this.persisted = persisted;
        this.closeWithinContext = closeWithinContext;
        builder = ChronicleMap
                .of(IntValue.class, String.class)
                .valueReaderAndDataAccess(new CountedStringReader(), new StringUtf8DataAccess());
        if (replicated)
            builder.replication((byte) 1);
        builder.entries(1).averageValueSize(1);
    }

    @Before
    public void initNoBytesStore() {
        Assert.assertNotEquals(0, NoBytesStore.NO_PAGE);
    }

    @Before
    public void resetSerializerCount() {
        serializerCount.set(0);
    }

    @Test
    public void testChronicleMapCollectedAndDirectMemoryReleased()
            throws IOException, InterruptedException {
        long nativeMemoryUsedBeforeMap = nativeMemoryUsed();
        int serializersBeforeMap = serializerCount.get();
        ChronicleMap<IntValue, String> map = getMap();
        long expectedNativeMemory = nativeMemoryUsedBeforeMap + map.offHeapMemoryUsed();
        assertEquals(expectedNativeMemory, nativeMemoryUsed());
        tryCloseFromContext(map);
        WeakReference<ChronicleMap<IntValue, String>> ref = new WeakReference<>(map);
        Assert.assertNotNull(ref.get());
        map = null;
        // Wait until Map is collected by GC
        while (ref.get() != null) {
            System.gc();
            Thread.yield();
        }
        // Wait until Cleaner is called and memory is returned to the system
        for (int i = 0; i < 6_000; i++) {
            if (nativeMemoryUsedBeforeMap == nativeMemoryUsed() &&
                    serializerCount.get() == serializersBeforeMap) {
                break;
            }
            System.gc();
            byte[] garbage = new byte[10_000_000];
            Thread.sleep(10);
        }
        Assert.assertEquals(nativeMemoryUsedBeforeMap, nativeMemoryUsed());
        Assert.assertEquals(serializersBeforeMap, serializerCount.get());
    }

    private long nativeMemoryUsed() {
        if (persisted) {
            return OS.memoryMapped();
        } else {
            return OS.memory().nativeMemoryUsed();
        }
    }

    @Test(timeout = 60_000)
    public void testExplicitChronicleMapCloseReleasesMemory()
            throws IOException, InterruptedException {
        long nativeMemoryUsedBeforeMap = nativeMemoryUsed();
        int serializersBeforeMap = serializerCount.get();
        ChronicleMap<IntValue, String> map = getMap();
        // One serializer should be copied to the map's valueReader field, another is copied from
        // the map's valueReader field to the context
        Assert.assertTrue(serializerCount.get() >= serializersBeforeMap + 2);
        Assert.assertNotEquals(0, map.offHeapMemoryUsed());
        try {
            long expectedNativeMemory = nativeMemoryUsedBeforeMap + map.offHeapMemoryUsed();
            assertEquals(expectedNativeMemory, nativeMemoryUsed());
        } finally {
            tryCloseFromContext(map);
            map.close();
        }
        assertEquals(nativeMemoryUsedBeforeMap, nativeMemoryUsed());
        // Wait until chronicle map context (hence serializers) is collected by the GC
        for (int i = 0; i < 6_000; i++) {
            if (serializerCount.get() == serializersBeforeMap + 1)
                break;
            System.gc();
            byte[] garbage = new byte[10_000_000];
            Thread.sleep(10);
        }
        Assert.assertEquals(serializersBeforeMap + 1, serializerCount.get());
        // This assertion ensures GC doesn't reclaim the map before or during the loop iteration
        // above, to ensure that we test that the direct memory and contexts are released because
        // of the manual map.close(), despite the "leak" of the map object itself.
        Assert.assertEquals(0, map.offHeapMemoryUsed());
        map = null;
        // After cleaning the reference to the map, the last serializer, which was held in it's
        // field, should be cleaned as well
        for (int i = 0; i < 6_000; i++) {
            if (serializerCount.get() == serializersBeforeMap)
                break;
            System.gc();
            byte[] garbage = new byte[10_000_000];
            Thread.sleep(10);
        }
        Assert.assertEquals(serializersBeforeMap, serializerCount.get());
    }

    private ChronicleMap<IntValue, String> getMap() throws IOException {
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

    private void tryCloseFromContext(ChronicleMap<IntValue, String> map) {
        // Test that the map could still be successfully closed and no leaks are introduced
        // by an attempt to close the map from within context.
        if (closeWithinContext) {
            IntValue key = Values.newHeapInstance(IntValue.class);
            try (ExternalMapQueryContext<IntValue, String, ?> c = map.queryContext(key)) {
                c.updateLock().lock();
                try {
                    map.close();
                } catch (IllegalStateException expected) {
                    // expected
                }
            }
        }
    }

    private class CountedStringReader extends StringSizedReader {

        private final String creationStackTrace;
        private final Cleaner cleaner;

        CountedStringReader() {
            serializerCount.incrementAndGet();
            serializers.add(new WeakReference<>(this));
            cleaner = Cleaner.create(this, serializerCount::decrementAndGet);
            try (StringWriter stringWriter = new StringWriter();
                 PrintWriter printWriter = new PrintWriter(stringWriter)) {
                new Exception().printStackTrace(printWriter);
                printWriter.flush();
                creationStackTrace = stringWriter.toString();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public CountedStringReader copy() {
            return new CountedStringReader();
        }
    }
}
