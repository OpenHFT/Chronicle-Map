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

package net.openhft.chronicle.map.fromdocs;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.hash.ChecksumEntry;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ExternalMapQueryContext;
import net.openhft.chronicle.map.MapEntry;
import net.openhft.chronicle.set.Builder;
import net.openhft.chronicle.values.Values;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assume.assumeFalse;

public class ChecksumEntryTest {

    @Test
    public void testChecksumEntriesWithValueInterface() throws IOException {
        assumeFalse(Jvm.isArm());
        File file = Builder.getPersistenceFile();

        try (ChronicleMap<Integer, LongValue> map = ChronicleMap
                .of(Integer.class, LongValue.class)
                .entries(1)
                // Entry checksums make sense only for persisted Chronicle Maps, and are ON by
                // default for such maps
                .createPersistedTo(file)) {

            LongValue value = Values.newHeapInstance(LongValue.class);
            value.setValue(42);
            map.put(1, value);

            try (ExternalMapQueryContext<Integer, LongValue, ?> c = map.queryContext(1)) {
                // Update lock required for calling ChecksumEntry.checkSum()
                c.updateLock().lock();
                MapEntry<Integer, LongValue> entry = c.entry();
                Assert.assertNotNull(entry);
                ChecksumEntry checksumEntry = (ChecksumEntry) entry;
                Assert.assertTrue(checksumEntry.checkSum());

                // to access off-heap bytes, should call value().getUsing() with Native value
                // provided. Simple get() return Heap value by default
                LongValue nativeValue =
                        entry.value().getUsing(Values.newNativeReference(LongValue.class));
                // This value bytes update bypass Chronicle Map internals, so checksum is not
                // updated automatically
                nativeValue.setValue(43);
                Assert.assertFalse(checksumEntry.checkSum());

                // Restore correct checksum
                checksumEntry.updateChecksum();
                Assert.assertTrue(checksumEntry.checkSum());
            }
        }
    }
}
