package net.openhft.chronicle.map.issue;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class ParallelStartupTest {
    @Ignore("Needs to be run manually in parallel - no JavaProcessBuilder")
    @Test
    public void test() {
        try {
            final File file = new File("issue342");
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

            for (int i = 0; i < thread.length; i++)
                thread[i].start();

            for (int i = 0; i < thread.length; i++)
                thread[i].join();

            file.delete();

            assertEquals(thread.length, succ.get());
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
    }
}