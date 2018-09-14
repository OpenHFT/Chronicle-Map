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

package net.openhft.chronicle.map;

import net.openhft.chronicle.core.Jvm;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by Borislav Ivanov on 5/29/15.
 */
public class ChronicleMapSanityCheckTest {

    @Test
    public void testSanity1() throws IOException, InterruptedException {

        String tmp = System.getProperty("java.io.tmpdir");

        String pathname = tmp + "/testSanity1-" + UUID.randomUUID().toString() + ".dat";

        File file = new File(pathname);

        System.out.println("Starting sanity test 1. Chronicle file :" +
                file.getAbsolutePath().toString());

        ScheduledExecutorService producerExecutor =
                Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors() - 1);

        ScheduledExecutorService consumerExecutor =
                Executors.newSingleThreadScheduledExecutor();

        int N = 1000;

        int producerPeriod = 100;
        TimeUnit producerTimeUnit = TimeUnit.MILLISECONDS;

        int consumerPeriod = 100;
        TimeUnit consumerTimeUnit = TimeUnit.MILLISECONDS;

        int totalTestTimeMS = (consumerPeriod + producerPeriod) * 20;

        try (ChronicleMap<String, DummyValue> map =
                     ChronicleMapBuilder.of(String.class, DummyValue.class)
                             .averageKey("" + N).averageValue(DummyValue.DUMMY_VALUE)
                             .entries(N)
                             .createPersistedTo(file)) {

            map.clear();

            producerExecutor.scheduleAtFixedRate(() -> {

                Thread.currentThread().setName("Producer " + Thread.currentThread().getId());
                Random r = new Random();

                System.out.println("Before PRODUCING size is " + map.size());
                for (int i = 0; i < N; i++) {
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
