/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map;

import net.openhft.lang.io.Bytes;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

public class CHMMetaDataTest {
    private static final String TMP = System.getProperty("java.io.tmpdir");

    @Test
    public void testAccessTimes() throws IOException {
        File file = new File(TMP, "testAccessTimes");
        MapEventListener<String, String, ChronicleMap<String, String>> listener =
                new StringStringMapEventListener(new AtomicLong(1));
        ChronicleMap<String, String> map = ChronicleMapBuilder.of(String.class, String.class)
                .metaDataBytes(8)
                .eventListener(listener).create();

        try {
            map.put("a", "aye");
            map.put("b", "bee");
            map.put("c", "see");
            map.put("d", "dee");
//            assertEquals(5, timeStamps.longValue());
            assertEquals("aye", map.get("a"));
            assertEquals("bee", map.get("b"));
            assertEquals("see", map.get("c"));
            assertEquals("dee", map.get("d"));
//            assertEquals(9, timeStamps.longValue());
            assertEquals("aye", map.remove("a"));
            assertEquals("bee", map.remove("b"));
            assertEquals("see", map.remove("c"));
            assertEquals("dee", map.remove("d"));
//            assertEquals(9, timeStamps.longValue());
        } finally {
            map.close();
            file.delete();
        }

    }

    private static class StringStringMapEventListener
            extends MapEventListener<String, String, ChronicleMap<String, String>> {
        private static final long serialVersionUID = 0L;

        private final AtomicLong timeStamps;

        public StringStringMapEventListener(AtomicLong timeStamps) {
            this.timeStamps = timeStamps;
        }

        @Override
        public void onGetFound(ChronicleMap<String, String> map, Bytes entry, int metaDataBytes,
                               String key, String value) {
            assertEquals(8, metaDataBytes);
            entry.writeLong(0, timeStamps.incrementAndGet());
        }

        @Override
        public void onPut(ChronicleMap<String, String> map, Bytes entry, int metaDataBytes,
                          boolean added, String key, String value, long pos,
                          SharedSegment segment) {
            assertEquals(8, metaDataBytes);
            if (added)
                assertEquals(0, entry.readLong());
            entry.writeLong(0, timeStamps.incrementAndGet());
        }

        @Override
        public void onRemove(ChronicleMap<String, String> map, Bytes entry, int metaDataBytes,
                             String key, String value, long pos, SharedSegment segment) {
            assertEquals(8, metaDataBytes);
            System.out.println("Removed " + key + "/" + value + " with ts of " + entry.readLong(0));
        }
    }
}
