/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import java.io.File;
import java.io.IOException;

@SuppressWarnings({"rawtypes", "unchecked"})
public final class MapDiagnostics {

    private MapDiagnostics() {
    }

    public static void main(String[] args) throws IOException {
        String mapFile = args[0];
        try (ChronicleMap map = ChronicleMap.of(Object.class, Object.class)
                .createPersistedTo(new File(mapFile))) {
            printMapStats(map);
        }
    }

    public static <K, V> void printMapStats(ChronicleMap<K, V> map) {
        for (int i = 0; i < map.segments(); i++) {
            try (MapSegmentContext<K, V, ?> c = map.segmentContext(i)) {
                System.out.printf("segment %d contains %d entries\n", i, c.size());
                c.forEachSegmentEntry(e -> System.out.printf("%s, %d bytes -> %s, %d bytes\n",
                        e.key(), e.key().size(), e.value(), e.value().size()));
            }
        }
    }
}
