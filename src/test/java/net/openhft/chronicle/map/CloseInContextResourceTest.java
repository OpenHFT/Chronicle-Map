/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.Assert.*;

public class CloseInContextResourceTest {
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void rejectedQueryCloseCanReleasePersistedMap() throws IOException {
        assertRejectedClose(false, false);
    }

    @Test
    public void rejectedQueryCloseCanReleaseReplicatedPersistedMap() throws IOException {
        assertRejectedClose(true, false);
    }

    @Test
    public void rejectedIterationCloseCanReleasePersistedMap() throws IOException {
        assertRejectedClose(false, true);
    }

    @Test
    public void rejectedIterationCloseCanReleaseReplicatedPersistedMap() throws IOException {
        assertRejectedClose(true, true);
    }

    private void assertRejectedClose(boolean replicated, boolean iteration) throws IOException {
        File file = temporaryFolder.newFile();
        ChronicleMapBuilder<Integer, Integer> builder = ChronicleMap
                .of(Integer.class, Integer.class).entries(1);
        if (replicated)
            builder.replication((byte) 1);
        try (ChronicleMap<Integer, Integer> map = builder.createPersistedTo(file)) {
            map.put(1, 2);
            if (iteration) {
                map.forEachEntry(entry -> assertRejected(map));
            } else {
                try (ExternalMapQueryContext<Integer, Integer, ?> context = map.queryContext(1)) {
                    context.updateLock().lock();
                    assertRejected(map);
                    assertEquals(Integer.valueOf(2), context.entry().value().get());
                }
            }
            // The unsuccessful close must leave the map usable and allow normal close afterwards.
            assertEquals(Integer.valueOf(2), map.get(1));
            map.put(2, 3);
        }
        // Windows requires all mappings and file handles to be released before deletion.
        Files.delete(file.toPath());
        assertFalse(file.exists());
    }

    private static void assertRejected(ChronicleMap<Integer, Integer> map) {
        IllegalStateException error = assertThrows(IllegalStateException.class, map::close);
        assertTrue(error.getMessage().contains("not yet finished query or iteration"));
        assertTrue("Rejected close must leave the map open", map.isOpen());
    }
}
