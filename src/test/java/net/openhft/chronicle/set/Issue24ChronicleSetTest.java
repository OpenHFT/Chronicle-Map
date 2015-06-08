/*
 * Copyright 2015 Higher Frequency Trading
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

package net.openhft.chronicle.set;

import net.openhft.chronicle.hash.ChronicleHash;
import net.openhft.chronicle.hash.ChronicleHashBuilder;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.samskivert.util.CollectionUtil.selectRandomSubset;

public class Issue24ChronicleSetTest {

    public static <H extends ChronicleHash,  B extends ChronicleHashBuilder<K, H, B>, K>
    H init(B builder, int entrySize, int averageKeySize, String fileName) {

        String tmp = System.getProperty("java.io.tmpdir");
        String pathname = tmp + "/" + fileName;
        File file = new File(pathname);
        try {
            H result = builder.entries(entrySize)
                    .averageKeySize(averageKeySize).createPersistedTo(file);
            return result;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public synchronized static <A> ChronicleSet<A> initSet(
            Class<A> entryClass, int entrySize, int averageKeySize, String fileName) {
        return init(ChronicleSetBuilder.of(entryClass), entrySize, averageKeySize, fileName);
    }

    @Test
    public void issue24ChronicleSetTest() {
        ChronicleSet<String> set = initSet(String.class, 1_000_000, 30, "stringSet.dat");
        ExecutorService executor = Executors.newFixedThreadPool(5);
        for (int i = 0; i < 10; i++) {
            Runnable worker = new WorkerThread(set);
            executor.execute(worker);
        }
        executor.shutdown();
        while (!executor.isTerminated()) {

        }
        System.out.println("Finished all threads");


    }

    public static class WorkerThread implements Runnable {
        private final ChronicleSet<String> set;
        public WorkerThread(ChronicleSet<String> set){
            this.set = set;
        }

        @Override
        public void run() {
            System.out.println(Thread.currentThread().getName()+" Start. Command = " + set.size());
            processCommand();
            System.out.println(Thread.currentThread().getName()+" End.");
        }

        private void processCommand() {
            Set<String> nomenclatures = new HashSet<>();
            for(int i = 0; i < 10; i++) {
                String nomenclature = "#############################" + i;
                nomenclatures.add(nomenclature);

            }

            set.addAll(nomenclatures);


            Set<String> strings = new HashSet<>(
                    selectRandomSubset(nomenclatures, nomenclatures.size() / 2));

            set.addAll(strings);


            Set<String> toRemove = new HashSet<>();
            Random generator = new Random();
            for (int j = 0; j < 3; j++) {
                int i = generator.nextInt(10);
                String nomenclature = "#############################" + i;
                toRemove.add(nomenclature);
            }
            set.removeAll(toRemove);

            for (String s : set) {
                System.out.println(s);
            }

            strings = new HashSet<>(selectRandomSubset(
                    nomenclatures, nomenclatures.size() / 2));

            set.addAll(strings);

            for (String s : set) {
                System.out.println(s);
            }
        }

    }
}
