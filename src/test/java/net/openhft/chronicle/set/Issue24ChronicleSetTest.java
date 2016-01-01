/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.set;

import net.openhft.chronicle.hash.ChronicleHash;
import net.openhft.chronicle.hash.ChronicleHashBuilder;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
    public void issue24ChronicleSetTest() throws ExecutionException, InterruptedException {
        ChronicleSet<String> set = initSet(String.class, 1_000_000, 30, "stringSet.dat");
        ExecutorService executor = Executors.newFixedThreadPool(5);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Runnable worker = new WorkerThread(set);
            futures.add(executor.submit(worker));
        }
        executor.shutdown();
        for (Future<?> future : futures) {
            future.get();
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
            for (Iterator<String> iterator = set.iterator(); iterator.hasNext(); ) {
                try {
                    String s = iterator.next();
                    if (toRemove.contains(s))
                        iterator.remove();
                } catch (NoSuchElementException e) {
                    // ignore
                }
            }

            for (Iterator<String> iterator = set.iterator(); iterator.hasNext(); ) {
                try {
                    String s = iterator.next();
                    System.out.println(s);
                } catch (NoSuchElementException e) {
                    // ignore
                }
            }

            strings = new HashSet<>(selectRandomSubset(
                    nomenclatures, nomenclatures.size() / 2));

            set.addAll(strings);

            for (Iterator<String> iterator = set.iterator(); iterator.hasNext(); ) {
                try {
                    String s = iterator.next();
                    System.out.println(s);
                } catch (NoSuchElementException e) {
                    // ignore
                }
            }
        }
    }
}
