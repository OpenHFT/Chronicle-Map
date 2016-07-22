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

package net.openhft.chronicle.set;

import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class SetEntryOperationsTest {

    @Test
    public void setEntryOperationsTest() {
        AtomicInteger insertCounter = new AtomicInteger();
        AtomicInteger remoteCounter = new AtomicInteger();
        try (ChronicleSet<String> fruits = ChronicleSet
                .of(String.class)
                .entries(3)
                .averageKey("apple")
                .entryOperations(new SetEntryOperations<String, Void>() {
                    @Override
                    public Void remove(@NotNull SetEntry<String> entry) {
                        remoteCounter.addAndGet(1);
                        entry.doRemove();
                        return null;
                    }

                    @Override
                    public Void insert(@NotNull SetAbsentEntry<String> absentEntry) {
                        insertCounter.addAndGet(1);
                        absentEntry.doInsert();
                        return null;
                    }
                })
                .create()) {
            fruits.add("apple");
            fruits.add("banana");
            fruits.remove("banana");
            fruits.remove("grapes");

            Assert.assertEquals(2, insertCounter.get());
            Assert.assertEquals(1, remoteCounter.get());
        }
    }
}
