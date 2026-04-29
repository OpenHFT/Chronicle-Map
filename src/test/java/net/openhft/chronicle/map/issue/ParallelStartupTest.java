/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.issue;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class ParallelStartupTest {

    @RepeatedTest(5)
    public void test() {
        try {
            final File file = IOTools.createTempFile("issue342");
            Thread[] thread = new Thread[16];
            AtomicInteger succ = new AtomicInteger();
            for (int i = 0; i < thread.length; i++) {
                final int ii = i;
                thread[i] = new Thread(() -> {
                    ChronicleMap<Integer, CharSequence> map;
                    try {
                        map = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                                .entries(100)
                                .averageValueSize(100)
                                .createPersistedTo(file);

                        map.put(ii, Thread.currentThread().getName());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    succ.incrementAndGet();

                    map.close();
                });
            }

            for (Thread item : thread) item.start();

            for (Thread value : thread) value.join();

            Assertions.assertEquals(thread.length, succ.get());
        } catch (Exception ex) {
            ex.printStackTrace();
            Assertions.fail();
        }
    }

}
