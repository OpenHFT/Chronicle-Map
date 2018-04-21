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

    public static <K, H extends ChronicleHash<K, ?, ?, ?>, B extends ChronicleHashBuilder<K, H, B>>
    H init(B builder, int entrySize, int averageKeySize) throws IOException {

        File file = File.createTempFile("stringSet", ".dat");
        file.deleteOnExit();
        try {
            H result = builder.entries(entrySize)
                    .averageKeySize(averageKeySize).createPersistedTo(file);
            return result;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public synchronized static <A> ChronicleSet<A> initSet(
            Class<A> entryClass, int entrySize, int averageKeySize)
            throws IOException {
        return init(ChronicleSetBuilder.of(entryClass), entrySize, averageKeySize);
    }

    @Test
    public void issue24ChronicleSetTest() throws IOException {
        ChronicleSet<String> set = initSet(String.class, 1_000_000, 30);
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

        public WorkerThread(ChronicleSet<String> set) {
            this.set = set;
        }

        @Override
        public void run() {
            System.out.println(Thread.currentThread().getName() + " Start. Command = " + set.size());
            processCommand();
            System.out.println(Thread.currentThread().getName() + " End.");
        }

        private void processCommand() {
            Set<String> nomenclatures = new HashSet<>();
            for (int i = 0; i < 10; i++) {
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
