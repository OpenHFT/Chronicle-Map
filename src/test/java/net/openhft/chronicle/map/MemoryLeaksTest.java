/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import com.google.common.collect.Lists;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.values.IntValue;
import net.openhft.chronicle.hash.impl.util.Cleaner;
import net.openhft.chronicle.hash.impl.util.CleanerUtils;
import net.openhft.chronicle.hash.serialization.impl.StringSizedReader;
import net.openhft.chronicle.hash.serialization.impl.StringUtf8DataAccess;
import net.openhft.chronicle.values.Values;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.ref.WeakReference;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.*;

@SuppressWarnings({"rawtypes", "unchecked"})
class MemoryLeaksTest {

    /**
     * Accounting {@link CountedStringReader} creation and finalization. All serializers,
     * created since the map creation, should become unreachable after map.close() or collection by
     * Cleaner, it means that map contexts (referencing serializers) are collected by the GC
     */
    private final AtomicInteger serializerCount = new AtomicInteger();
    private final List<WeakReference<CountedStringReader>> serializers = new ArrayList<>();
    @TempDir
    Path tempDir;

    public static Collection<Object[]> data() {
        List<Boolean> booleans = Arrays.asList(false, true);
        // Test with all possible combinations of three boolean parameters.
        return Lists.cartesianProduct(booleans, booleans, booleans)
                .stream().map(flags -> {
                    final ArrayList<Object> namedFlags = new ArrayList<>(Collections.singletonList(named(flags)));
                    namedFlags.addAll(flags);
                    return namedFlags;
                }).map(List::toArray).collect(Collectors.toList());
    }

    private static String named(final List<Boolean> flags) {
        return (!flags.get(0) ? "not " : "") + "replicated, " +
                (!flags.get(1) ? "not " : "") + "persisted, " +
                (!flags.get(2) ? "not " : "") + "closed within context";
    }

    @BeforeEach
    void resetSerializerCount() {
        System.err.println("This test is expect to print 'ChronicleMap ... is not closed manually, cleaned up from Cleaner'");
        serializerCount.set(0);
    }

    @Timeout(10)
    @ParameterizedTest
    @MethodSource("data")
    void testChronicleMapCollectedAndDirectMemoryReleased(String testType, boolean replicated, boolean persisted, boolean closeWithinContext) throws IOException {
        assumeFalse(OS.isMacOSX());
        assumePersistedMapSupport(persisted);
        ChronicleMapBuilder<IntValue, String> builder = createBuilder(replicated);
        // This test is flaky in Linux and Mac OS apparently because some native memory from
        // running previous/concurrent tests is released during this test, that infers with
        // the (*) check below. The aim of this test is to check that native memory is not
        // leaked and it is proven if it succeeds at least sometimes at least in some OSes.
        // This tests is successful always in Windows and successful in Linux and OS X when run
        // alone, rather than along all other Chronicle Map's tests.

        System.gc();
        Jvm.pause(100);

        long nativeMemoryUsedBeforeMap = nativeMemoryUsed(persisted);
        int serializersBeforeMap = serializerCount.get();
        // the purpose of the test is to find maps which are not closed properly.
        ChronicleMap<IntValue, String> map = getMap(builder, persisted);
        long expectedNativeMemory = nativeMemoryUsedBeforeMap + map.offHeapMemoryUsed();
        try {
            long actual = nativeMemoryUsed(persisted);
            if (actual != expectedNativeMemory + 4096)
                assertEquals(expectedNativeMemory, actual);
        } finally {
            tryCloseFromContext(map, closeWithinContext);
        }
        WeakReference<ChronicleMap<IntValue, String>> ref = new WeakReference<>(map);
        assertNotNull(ref.get());
        //noinspection UnusedAssignment
        map = null;

        // Wait until Map is collected by GC
        // Wait until Cleaner is called and memory is returned to the system
        for (int i = 1; i <= 10; i++) {
            if (ref.get() == null &&
                    nativeMemoryUsedBeforeMap >= nativeMemoryUsed(persisted) && // (*)
                    serializerCount.get() == serializersBeforeMap) {
                break;
            }
            System.gc();
            Jvm.pause(i * 10);
            System.out.println("ref.get()=" + (ref.get() == null));
            System.out.println(nativeMemoryUsedBeforeMap + " <=> " + nativeMemoryUsed(persisted));
            System.out.println(serializerCount.get() + " <=> " + serializersBeforeMap);
        }
        if (nativeMemoryUsedBeforeMap < nativeMemoryUsed(persisted)) {
            long actual = waitForNativeMemoryAtMost(persisted, nativeMemoryUsedBeforeMap + 4096);
            if (actual != nativeMemoryUsedBeforeMap + 4096) {
                assertEquals(nativeMemoryUsedBeforeMap, actual);
            }
        }
        assertEquals(serializersBeforeMap, serializerCount.get());
    }

    private long nativeMemoryUsed(boolean persisted) {
        if (persisted) {
            return OS.memoryMapped();
        } else {
            return OS.memory().nativeMemoryUsed();
        }
    }

    private long waitForNativeMemoryAtMost(boolean persisted, long maxAllowed) {
        long actual = nativeMemoryUsed(persisted);
        for (int i = 1; actual > maxAllowed && i <= 40; i++) {
            System.gc();
            Jvm.pause(i * 25L);
            actual = nativeMemoryUsed(persisted);
        }
        return actual;
    }

    @Timeout(60)
    @ParameterizedTest
    @MethodSource("data")
    void testExplicitChronicleMapCloseReleasesMemory(String testType, boolean replicated, boolean persisted, boolean closeWithinContext)
            throws IOException, InterruptedException {
        assumePersistedMapSupport(persisted);
        ChronicleMapBuilder<IntValue, String> builder = createBuilder(replicated);
        long nativeMemoryUsedBeforeMap = nativeMemoryUsed(persisted);
        int serializersBeforeMap = serializerCount.get();
        try (ChronicleMap<IntValue, String> map = getMap(builder, persisted)) {
            // One serializer should be copied to the map's valueReader field, another is copied from
            // the map's valueReader field to the context
            assertTrue(serializerCount.get() >= serializersBeforeMap + 2);
            assertNotEquals(0, map.offHeapMemoryUsed());
            try {
                long expectedNativeMemory = nativeMemoryUsedBeforeMap + map.offHeapMemoryUsed();
                assertEquals(expectedNativeMemory, nativeMemoryUsed(persisted),
                        String.format(
                                "used before map: %d, used by map: %d, expected used: %d, actual used: %d",
                                nativeMemoryUsedBeforeMap,
                                map.offHeapMemoryUsed(),
                                expectedNativeMemory, nativeMemoryUsed(persisted)));
            } finally {
                tryCloseFromContext(map, closeWithinContext);
                Closeable.closeQuietly(map);
            }

            if (closeWithinContext) {
                // Fails because of https://github.com/OpenHFT/Chronicle-Map/issues/153
                return;
            } else {
                long actual = waitForNativeMemoryAtMost(persisted, nativeMemoryUsedBeforeMap + 4096);
                if (actual != 16_000)
                    assertEquals(nativeMemoryUsedBeforeMap, actual);
            }

            // Wait until chronicle map context (hence serializers) is collected by the GC
            for (int i = 0; i < 6_000; i++) {
                if (serializerCount.get() == serializersBeforeMap)
                    break;
                System.gc();
                byte[] garbage = new byte[50_000_000];
                Thread.sleep(1);
            }
            assertTrue(serializerCount.get() == serializersBeforeMap);
            // This assertion ensures GC doesn't reclaim the map before or during the loop iteration
            // above, to ensure that we test that the direct memory and contexts are released because
            // of the manual map.close(), despite the "leak" of the map object itself.

            // Assertion disabled because a closed map now guards offHeapMemoryUsed()
            //assertEquals(0, map.offHeapMemoryUsed());
        }
    }

    private ChronicleMapBuilder<IntValue, String> createBuilder(boolean replicated) {
        ChronicleMapBuilder<IntValue, String> builder = ChronicleMap
                .of(IntValue.class, String.class).constantKeySizeBySample(Values.newHeapInstance(IntValue.class))
                .valueReaderAndDataAccess(new CountedStringReader(this), new StringUtf8DataAccess());
        if (replicated)
            builder.replication((byte) 1);
        builder.entries(1).averageValueSize(10);
        return builder;
    }

    private ChronicleMap<IntValue, String> getMap(ChronicleMapBuilder<IntValue, String> builder, boolean persisted) throws IOException {
        VanillaChronicleMap<IntValue, String, ?> map;
        if (persisted) {
            map = (VanillaChronicleMap<IntValue, String, ?>)
                    builder.createPersistedTo(File.createTempFile("tmp", null, tempDir.toFile()));
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

    private void assumePersistedMapSupport(boolean persisted) {
        if (!persisted) {
            return;
        }
        assumeTrue(isUnmapSupportAvailable(),
                "Skipping persisted variants because chronicle-core unmap support is unavailable in this JVM");
    }

    private boolean isUnmapSupportAvailable() {
        try {
            Method getUnmapp0Mh = OS.class.getDeclaredMethod("getUnmapp0Mh");
            getUnmapp0Mh.setAccessible(true);
            getUnmapp0Mh.invoke(null);
            return true;
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new AssertionError("Unable to probe unmap support", e);
        } catch (InvocationTargetException | LinkageError e) {
            return false;
        }
    }

    private void tryCloseFromContext(ChronicleMap<IntValue, String> map, boolean closeWithinContext) {
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

    private static final class CountedStringReader extends StringSizedReader {
        private transient MemoryLeaksTest memoryLeaksTest;
        private final String creationStackTrace;
        private final Cleaner cleaner;

        CountedStringReader(MemoryLeaksTest memoryLeaksTest) {
            this.memoryLeaksTest = memoryLeaksTest;
            this.memoryLeaksTest.serializerCount.incrementAndGet();
            this.memoryLeaksTest.serializers.add(new WeakReference<>(this));
            cleaner = CleanerUtils.createCleaner(this, this.memoryLeaksTest.serializerCount::decrementAndGet);
            try (StringWriter stringWriter = new StringWriter();
                 PrintWriter printWriter = new PrintWriter(stringWriter)) {
                new Exception().printStackTrace(printWriter);
                printWriter.flush();
                creationStackTrace = stringWriter.toString();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            this.memoryLeaksTest = memoryLeaksTest;
        }

        @Override
        public CountedStringReader copy() {
            return new CountedStringReader(this.memoryLeaksTest);
        }
    }
}
