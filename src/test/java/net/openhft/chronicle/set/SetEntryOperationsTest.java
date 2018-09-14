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
