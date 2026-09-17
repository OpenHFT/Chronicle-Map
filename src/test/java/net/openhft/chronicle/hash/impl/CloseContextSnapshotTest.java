/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.UnsafeMemory;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.VanillaChronicleMap;
import org.junit.Test;

import java.lang.ref.WeakReference;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CloseContextSnapshotTest {
    @Test
    public void closeWithoutInitialisedResources() throws InstantiationException {
        // Wire allocates an instance before reading its fields and calling initTransients().
        VanillaChronicleMap<?, ?, ?> map =
                (VanillaChronicleMap<?, ?, ?>) UnsafeMemory.UNSAFE.allocateInstance(VanillaChronicleMap.class);
        map.close();
        assertTrue(map.isClosed());
    }

    @Test
    @SuppressWarnings("try") // Inspect explicit close; try-with-resources cleans up a failing control.
    public void closeIgnoresReleasedContextSlots() {
        try (ChronicleMap<Integer, Integer> map = ChronicleMap.of(Integer.class, Integer.class).entries(1).create()) {
            map.put(1, 2);
            ChronicleHashResources resources = Jvm.getValue(map, "resources");
            List<WeakReference<ContextHolder>> contexts = resources.contexts();
            // closeContexts() nulls released slots; weak references can also lose their holders.
            WeakReference<ContextHolder> collected = new WeakReference<>(null);
            contexts.add(null);
            contexts.add(collected);
            try {
                map.close();
                assertTrue(map.isClosed());
                assertEquals(0L, resources.totalMemory());
            } finally {
                // Keep a failing control from leaking the map into subsequent tests.
                contexts.remove(null);
                contexts.remove(collected);
            }
        }
    }

    @Test
    @SuppressWarnings("try") // Inspect explicit close; try-with-resources cleans up a failing control.
    public void closeIgnoresClearedContextHolders() {
        try (ChronicleMap<Integer, Integer> map = ChronicleMap.of(Integer.class, Integer.class).entries(1).create()) {
            map.put(1, 2);
            ChronicleHashResources resources = Jvm.getValue(map, "resources");
            List<WeakReference<ContextHolder>> contexts = resources.contexts();
            ContextHolder cleared = new ContextHolder(contexts.get(0).get().get());
            cleared.clear();
            WeakReference<ContextHolder> released = new WeakReference<>(cleared);
            contexts.add(released);
            try {
                map.close();
                assertTrue(map.isClosed());
                assertEquals(0L, resources.totalMemory());
            } finally {
                contexts.remove(released);
            }
            assertNull(cleared.get());
        }
    }
}
