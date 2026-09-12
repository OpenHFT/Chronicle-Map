/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.threads.NamedThreadFactory;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by Borislav Ivanov on 5/29/15.
 */
class ChronicleMapSanityCheckTest {

    @Test
    void testSanity1() throws IOException, InterruptedException {

        String tmp = OS.getTarget();

        String pathname = tmp + "/testSanity1-" + Time.uniqueId() + ".dat";

        File file = new File(pathname);

        System.out.println("Starting sanity test 1. Chronicle file :" +
                file.getAbsolutePath());

        ScheduledExecutorService producerExecutor =
                Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors() - 1,
                        new NamedThreadFactory("producer"));

        ScheduledExecutorService consumerExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        new NamedThreadFactory("consumer"));

        int iterations = 1000;

        int producerPeriod = 100;
        TimeUnit producerTimeUnit = TimeUnit.MILLISECONDS;

        int consumerPeriod = 100;
        TimeUnit consumerTimeUnit = TimeUnit.MILLISECONDS;

        int totalTestTimeMS = (consumerPeriod + producerPeriod) * 20;

        try (ChronicleMap<String, DummyValue> map =
                     ChronicleMapBuilder.of(String.class, DummyValue.class)
                             .averageKey("" + iterations).averageValue(DummyValue.DUMMY_VALUE)
                             .entries(iterations)
                             .createPersistedTo(file)) {

            map.clear();

            producerExecutor.scheduleAtFixedRate(() -> {

                Thread.currentThread().setName("Producer " + Jvm.currentThreadId());
                Random r = new Random();

                System.out.println("Before PRODUCING size is " + map.size());
                for (int i = 0; i < iterations; i++) {
                    LockSupport.parkNanos(r.nextInt(5));
                    map.put(String.valueOf(i), DummyValue.DUMMY_VALUE);
                }
                System.out.println("After PRODUCING size is " + map.size());

            }, 0, producerPeriod, producerTimeUnit);

            consumerExecutor.scheduleAtFixedRate(() -> {

                Thread.currentThread().setName("Consumer");
                Set<String> keys = map.keySet();

                Random r = new Random();

                System.out.println("Before CONSUMING size is " + map.size());
                System.out.println();
                for (String key : keys) {
                    if (r.nextBoolean()) {
                        map.remove(key);
                    }
                }

                System.out.println("After CONSUMING size is " + map.size());

            }, 0, consumerPeriod, consumerTimeUnit);

            Jvm.pause(totalTestTimeMS);

            consumerExecutor.shutdown();
            try {
                consumerExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            producerExecutor.shutdown();
            try {
                producerExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    enum DummyValue {
        DUMMY_VALUE
    }
}
