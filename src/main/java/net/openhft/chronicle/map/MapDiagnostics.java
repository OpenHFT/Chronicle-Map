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

import java.io.File;
import java.io.IOException;

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
