/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
        BytesMapEventListener listener =
                new StringStringMapEventListener(new AtomicLong(1));
        ChronicleMap<String, String> map = ChronicleMapBuilder.of(String.class, String.class)
                .metaDataBytes(8).bytesEventListener(listener).create();

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
            extends BytesMapEventListener {
        private static final long serialVersionUID = 0L;

        private final AtomicLong timeStamps;

        public StringStringMapEventListener(AtomicLong timeStamps) {
            this.timeStamps = timeStamps;
        }

        @Override
        public void onGetFound(Bytes entry, long metaDataPos, long keyPos, long valuePos) {
            entry.writeLong(metaDataPos, timeStamps.incrementAndGet());
        }

        @Override
        public void onPut(Bytes entry, long metaDataPos, long keyPos, long valuePos, boolean added, boolean replicationEvent, boolean hasValueChanged, byte identifier, byte replacedIdentifier, long timeStamp, long replacedTimeStamp, SharedSegment segment) {
            if (added)
                assertEquals(0, entry.readLong(metaDataPos));
            entry.writeLong(metaDataPos, timeStamps.incrementAndGet());
        }

        @Override
        public void onRemove(Bytes entry, long metaDataPos, long keyPos, long valuePos, boolean replicationEvent, byte identifier, byte replacedIdentifier, long timeStamp, long replacedTimeStamp, SharedSegment segment) {
            System.out.println("Removed entry with ts of " + entry.readLong(metaDataPos));
        }
    }
}
