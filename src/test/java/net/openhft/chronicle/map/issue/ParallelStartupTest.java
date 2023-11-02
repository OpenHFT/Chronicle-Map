package net.openhft.chronicle.map.issue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.testframework.process.JavaProcessBuilder;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;

public class ParallelStartupTest {
    @Test
    public void test() throws InterruptedException {
        assumeFalse(OS.isWindows());//TODO: unstable on Windows

        Process processOne = JavaProcessBuilder.create(ParallelStartupTest.class).start();
        Process processTwo = JavaProcessBuilder.create(ParallelStartupTest.class).start();

        processTwo.waitFor();
        processOne.waitFor();

        JavaProcessBuilder.printProcessOutput("ParallelStartupTest", processOne);
        JavaProcessBuilder.printProcessOutput("ParallelStartupTest", processTwo);

        assertEquals("Process terminated with error", 0, processTwo.exitValue());
        assertEquals("Process terminated with error", 0, processOne.exitValue());
    }

    public static void main(String[] args) throws InterruptedException {
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

            for (int i = 0; i < thread.length; i++)
                thread[i].start();

            for (int i = 0; i < thread.length; i++)
                thread[i].join();

            assertEquals(thread.length, succ.get());
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
    }
}