/*
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

import com.google.common.io.Files;
import net.openhft.chronicle.set.Builder;
import org.junit.Test;

import java.io.File;
import java.net.URL;

import static org.junit.Assert.assertEquals;

public class ChronicleMap3_12IntegerKeyCompatibilityTest {

    @Test
    public void testWithChecksums() throws Exception {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        URL fileUrl = cl.getResource("chronicle-map-3-12-with-checksums.dat");
        File file = new File(fileUrl.toURI());
        File persistenceFile = Builder.getPersistenceFile();
        Files.copy(file, persistenceFile);
        try (ChronicleMap<Integer, String> map = ChronicleMap.of(Integer.class, String.class)
                .averageValue("1")
                .entries(1)
                .recoverPersistedTo(persistenceFile, false)) {
            assertEquals(2, map.size());
            assertEquals("1", map.get(1));
            assertEquals("-1", map.get(-1));
        }
    }

    @Test
    public void testNoChecksums() throws Exception {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        URL fileUrl = cl.getResource("chronicle-map-3-12-no-checksums.dat");
        File file = new File(fileUrl.toURI());
        File persistenceFile = Builder.getPersistenceFile();
        Files.copy(file, persistenceFile);
        try (ChronicleMap<Integer, String> map = ChronicleMap.of(Integer.class, String.class)
                .averageValue("1")
                .entries(1)
                .checksumEntries(false)
                .recoverPersistedTo(persistenceFile, false)) {
            assertEquals(2, map.size());
            assertEquals("1", map.get(1));
            assertEquals("-1", map.get(-1));
        }
    }
}
