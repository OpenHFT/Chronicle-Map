/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map;

import net.openhft.lang.io.DirectBytes;
import net.openhft.lang.io.DirectStore;
import net.openhft.lang.io.serialization.impl.SnappyStringMarshaller;
import org.junit.Ignore;
import org.junit.Test;

import java.beans.XMLEncoder;
import java.io.ByteArrayOutputStream;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by peter on 06/12/14.
 */
public class LargeEntriesTest {

    static final int ENTRIES = 640;
    static final int ENTRY_SIZE = 1024;

    @Test
    @Ignore
    public void testLargeStrings() throws ExecutionException, InterruptedException {
        final ChronicleMap<String, String> map = ChronicleMapBuilder
                .of(String.class, String.class)
                .valueMarshaller(SnappyStringMarshaller.INSTANCE)
                .actualSegments(1) // to force an error.
                .entries(ENTRIES * 2)
                .entrySize(ENTRY_SIZE / 4)
                .putReturnsNull(true)
                .create();
        {
            int threads = 2; //Runtime.getRuntime().availableProcessors();
            ExecutorService es = Executors.newFixedThreadPool(threads);
            final int block = ENTRIES / threads;
            for (int i = 0; i < 3; i++) {
                long start = System.currentTimeMillis();
                List<Future<?>> futureList = new ArrayList<>();
                for (int t = 0; t < threads; t++) {
                    final int finalT = t;
                    final int finalI = i;
                    futureList.add(es.submit(new Runnable() {
                        @Override
                        public void run() {
                            exerciseLargeStrings(map, finalI, finalT * block, finalT * block + block, ENTRY_SIZE);
                        }
                    }));
                }
                for (Future<?> future : futureList) {
                    future.get();
                }
                long time = System.currentTimeMillis() - start;
                long operations = 3;
                System.out.printf("Put/Get rate was %.1f MB/s%n", operations * ENTRIES * ENTRY_SIZE / 1e6 / (time / 1e3));
            }
            es.shutdown();
            if (es.isTerminated())
                map.close();
        }
    }

    void exerciseLargeStrings(ChronicleMap<String, String> map, int run, int start, int finish, int entrySize) {
/*
        final Thread thisThread = Thread.currentThread();
        Thread monitor = new Thread(new Runnable() {
            @Override
            public void run() {
                StringBuilder sb = new StringBuilder();
                sb.append(thisThread).append("\n");
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        Thread.sleep(50);
                        StackTraceElement[] stackTrace = thisThread.getStackTrace();
                        for (int i = 0; i < 4 && i < stackTrace.length; i++)
                            sb.append("\tat ").append(stackTrace[i]).append("\n");
                        sb.append("\n");
                    }
                } catch (InterruptedException e) {
                }
                System.out.println(sb);
            }
        });
        monitor.start();
*/
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        XMLEncoder xml = new XMLEncoder(baos);
        Map<String, String> map2 = new HashMap<>();
        Random rand = new Random(1);
        for (int i = 0; i < entrySize / 80; i++)
            map2.put("key-" + i, "value-" + rand.nextInt(1000));
        xml.writeObject(map2);
        xml.close();
        String value = baos.toString().substring(0, entrySize);
        // warmup to compression.
        if (run == 0) {
            DirectBytes bytes = DirectStore.allocate(entrySize).bytes();
            SnappyStringMarshaller.INSTANCE.write(bytes, value);
            bytes.flip();
            SnappyStringMarshaller.INSTANCE.read(bytes);
            bytes.release();
        }

        for (int i = start; i < finish; i++) {
            String key = "key-" + i;
            map.put(key, value);
            String object = map.get(key);

            assertNotNull(key, object);
            assertEquals(key, entrySize, object.length());
        }
//        monitor.interrupt();

        for (int i = start; i < finish; i++) {
//            System.out.println(i);
            String key = "key-" + i;
            String object = map.get(key);

            assertNotNull(key, object);
            assertEquals(key, entrySize, object.length());
        }
    }
}
