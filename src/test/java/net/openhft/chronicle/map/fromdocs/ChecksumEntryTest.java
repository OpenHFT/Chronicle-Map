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

package net.openhft.chronicle.map.fromdocs;

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

public class ChecksumEntryTest {

    @Test
    public void testChecksumEntriesWithValueInterface() throws IOException {
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
