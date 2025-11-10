//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

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
