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

import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.values.Values;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertNotNull;

/**
 * Created by peter.lawrey on 28/02/14.
 * <pre>
 * For 1M entries
 * run 1 1000000 : 50/90/99/99.9/99.99/worst: 1.0/3.2/14/18/116/172
 * run 1 500000 : 50/90/99/99.9/99.99/worst: 0.2/3.0/11/16/45/163
 * run 1 250000 : 50/90/99/99.9/99.99/worst: 0.2/3.0/10/16/20/155
 * run 1 100000 : 50/90/99/99.9/99.99/worst: 0.2/3.0/9.2/15/20/147
 * run 1 50000 : 50/90/99/99.9/99.99/worst: 0.2/3.0/9.2/15/20/139
 * </pre><pre>
 * For 1M entries to ext4 on laptop with SSD
 * run 1 2,000,000 : 50/90/99/99.9/99.99/worst : 0.2 / 0.7 / 8.7 / 15 / 60 / 129 micro-seconds.
 * run 1 1,000,000 : 50/90/99/99.9/99.99/worst : 0.2 / 0.5 / 6.6 / 10 / 15 / 43 micro-seconds.
 * run 1   500,000 : 50/90/99/99.9/99.99/worst : 0.2 / 0.5 / 5.6 / 10 / 15 / 53 micro-seconds.
 * </pre><pre>
 * For 1M entries on server to ext4 with PCI-SSD
 * run 1 1000000 : 50/90/99/99.9/99.99/worst: 0.9/3.2/14/18/106/188
 * run 1 500000 : 50/90/99/99.9/99.99/worst: 0.2/3.0/11/16/46/172
 * run 1 250000 : 50/90/99/99.9/99.99/worst: 0.2/3.0/10/16/20/163
 * run 1 100000 : 50/90/99/99.9/99.99/worst: 0.2/3.0/9.9/15/20/163
 * run 1 50000 : 50/90/99/99.9/99.99/worst: 0.2/3.0/9.9/15/20/147
 * </pre><pre>
 * For 10M entries
 * run 1 1000000 : 50/90/99/99.9/99.99/worst: 1.4/6.2/16/34/135/180
 * run 1 500000 : 50/90/99/99.9/99.99/worst: 0.3/3.1/11/17/54/159
 * run 1 250000 : 50/90/99/99.9/99.99/worst: 0.3/3.0/10/15/21/147
 * run 1 100000 : 50/90/99/99.9/99.99/worst: 0.4/3.2/9.7/15/20/151
 * run 1 50000 : 50/90/99/99.9/99.99/worst: 0.4/3.2/9.4/15/20/147
 * </pre><pre>
 * For 100M entries
 * run 1 1000000 : 50/90/99/99.9/99.99/worst: 570425/2818572/3355443/3422552/3489660/3556769
 * run 1 500000 : 50/90/99/99.9/99.99/worst: 1.1/11/27/43/94/184
 * run 1 250000 : 50/90/99/99.9/99.99/worst: 0.7/3.3/12/19/40/167
 * run 1 100000 : 50/90/99/99.9/99.99/worst: 0.7/3.3/10/16/21/151
 * run 1 50000 : 50/90/99/99.9/99.99/worst: 0.7/3.3/10/16/22/155
 * </pre>
 */
public class CHMLatencyTestMain {
    static final int KEYS = 1000 * 1000;
    static final int RUN_TIME = 30;
    static final long START_TIME = System.currentTimeMillis();

    // TODO test passes but is under development.
    public static void main(String... ignored) throws IOException {
        AffinityLock lock = AffinityLock.acquireCore();
        File file = File.createTempFile("testCHMLatency", "deleteme");
//        File file = new File("testCHMLatency.deleteme");
        file.delete();
        ChronicleMap<LongValue, LongValue> countersMap =
                ChronicleMapBuilder.of(LongValue.class, LongValue.class)
                        .entries(KEYS)
                        .createPersistedTo(file);

        // add keys
        LongValue key = Values.newHeapInstance(LongValue.class);
        LongValue value = Values.newNativeReference(LongValue.class);
        for (long i = 0; i < KEYS; i++) {
            key.setValue(i);
            countersMap.acquireUsing(key, value);
            value.setValue(0);
        }
        System.out.println("Keys created");
//        Monitor monitor = new Monitor();
        LongValue value2 = Values.newNativeReference(LongValue.class);
        for (int t = 0; t < 5; t++) {
            for (int rate : new int[]{2 * 1000 * 1000, 1000 * 1000, 500 * 1000/*, 250 * 1000, 100 * 1000, 50 * 1000*/}) {
                Histogram times = new Histogram();
                int u = 0;
                long start = System.nanoTime();
                long delay = 1000 * 1000 * 1000L / rate;
                long next = start + delay;
                for (long j = 0; j < RUN_TIME * rate; j += KEYS) {
                    int stride = Math.max(1, KEYS / (RUN_TIME * rate));
                    // the timed part
                    for (int i = 0; i < KEYS && u < RUN_TIME * rate; i += stride) {
                        // busy wait for next time.
                        while (System.nanoTime() < next - 12) ;
//                        monitor.sample = System.nanoTime();
                        long start0 = next;

                        // start the update.
                        key.setValue(i);
                        LongValue using = countersMap.getUsing(key, value2);
                        if (using == null)
                            assertNotNull(using);
                        value2.addAtomicValue(1);

                        // calculate the time using the time it should have started, not when it was able.
                        long elapse = System.nanoTime() - start0;
                        times.sample(elapse);
                        next += delay;
                    }
//                    monitor.sample = Long.MAX_VALUE;
                }
                System.out.printf("run %d %,9d : ", t, rate);
                times.printPercentiles(" micro-seconds.");
            }
            System.out.println();
        }
//        monitor.running = false;
        countersMap.close();
        file.delete();
    }

    static class Monitor implements Runnable {
        final Thread thread;
        volatile boolean running = true;
        volatile long sample;

        Monitor() {
            this.thread = Thread.currentThread();
            sample = Long.MAX_VALUE;
            new Thread(this).start();
        }

        @Override
        public void run() {
            while (running && !Thread.currentThread().isInterrupted()) {
                Jvm.pause(1);
                long delay = System.nanoTime() - sample;
                if (delay > 1000 * 1000) {
                    System.out.println("\n" + (System.currentTimeMillis() - START_TIME) + " : Delay of " + delay / 100000 / 10.0 + " ms.");
                    int count = 0;
                    for (StackTraceElement ste : thread.getStackTrace()) {
                        System.out.println("\tat " + ste);
                        if (count++ > 6) break;
                    }
                }
            }
        }
    }
}
