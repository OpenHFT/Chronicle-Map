package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.replication.TimeProvider;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.values.IntValue;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Rob Austin.
 */
public class NoTcpReplicationSoakTest {

    private ReplicatedChronicleMap<Integer, ?, ?, CharSequence, ?, ?> map1;
    private ReplicatedChronicleMap<Integer, ?, ?, CharSequence, ?, ?> map2;
    private IntValue value;
    static int s_port = 8010;
    private long time;

    TimeProvider timeProvider = new TimeProvider() {
        @Override
        public long currentTime() {
            return time;
        }
    };


    @Before
    public void setup() throws IOException {
        value = DataValueClasses.newDirectReference(IntValue.class);
        ((Byteable) value).bytes(new ByteBufferBytes(ByteBuffer.allocateDirect(4)), 0);


        {


            map1 = (ReplicatedChronicleMap) ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                    .replication((byte) 1)
                    .timeProvider(timeProvider)
                    .instance()
                    .name("map1")
                    .create();
        }
        {

            map2 = (ReplicatedChronicleMap) ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                    .replication((byte) 2)
                    .timeProvider(timeProvider)
                    .instance()
                    .name("map2")
                    .create();

        }
        s_port += 2;


    }


    @After
    public void tearDown() throws InterruptedException {

        for (final Closeable closeable : new Closeable[]{map1, map2}) {
            try {
                closeable.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        System.gc();
    }

    Set<Thread> threads;

    @Before
    public void sampleThreads() {
        threads = Thread.getAllStackTraces().keySet();
    }

    @After
    public void checkThreadsShutdown() {
        StatelessClientTest.checkThreadsShutdown(threads);
    }

    AtomicInteger task = new AtomicInteger();

    @Test
    public void testSoakTestWithRandomData() throws IOException, InterruptedException {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        System.out.print("SoakTesting ");
        for (int j = 1; j < 10000; j++) {
            if (j % 100 == 0)
                System.out.print(".");

            Random rnd = new Random(j);


            if (rnd.nextInt(10) < 2)
                time++;

            final long t = time;

            final int key = rnd.nextInt(1000);

            // add random delay
          addRandomDelay(executorService, rnd);

            if (rnd.nextBoolean()) {
                final CharSequence value = "test" + j;
                if (rnd.nextBoolean()) {
                    try (MapKeyContext<CharSequence> c = map1.context(key)) {
                        ReplicatedChronicleMap.ReplicatedContext rc =
                                (ReplicatedChronicleMap.ReplicatedContext) c;
                        rc.newTimestamp = t;
                        rc.newIdentifier = (byte) 1;
                        c.put(value);
                    }

                    task.incrementAndGet();
                    executorService.submit(new Runnable() {
                        @Override
                        public void run() {
                            try (MapKeyContext<CharSequence> c = map2.context(key)) {
                                ReplicatedChronicleMap.ReplicatedContext rc =
                                        (ReplicatedChronicleMap.ReplicatedContext) c;
                                rc.newTimestamp = t;
                                rc.newIdentifier = (byte) 1;
                                c.put(value);
                            }
                            task.decrementAndGet();
                        }
                    });

                } else {
                    try (MapKeyContext<CharSequence> c = map2.context(key)) {
                        ReplicatedChronicleMap.ReplicatedContext rc =
                                (ReplicatedChronicleMap.ReplicatedContext) c;
                        rc.newTimestamp = t;
                        rc.newIdentifier = (byte) 2;
                        c.put(value);
                    }

                    task.incrementAndGet();
                    executorService.submit(new Runnable() {
                        @Override
                        public void run() {
                            try (MapKeyContext<CharSequence> c = map1.context(key)) {
                                ReplicatedChronicleMap.ReplicatedContext rc =
                                        (ReplicatedChronicleMap.ReplicatedContext) c;
                                rc.newTimestamp = t;
                                rc.newIdentifier = (byte) 2;
                                c.put(value);
                            }
                            task.decrementAndGet();
                        }
                    });
                }

            } else {

                if (rnd.nextBoolean()) {
                    try (MapKeyContext<CharSequence> c = map1.context(key)) {
                        ReplicatedChronicleMap.ReplicatedContext rc =
                                (ReplicatedChronicleMap.ReplicatedContext) c;
                        rc.newTimestamp = t;
                        rc.newIdentifier = (byte) 1;
                        c.remove();
                    }

                    task.incrementAndGet();
                    executorService.submit(new Runnable() {
                        @Override
                        public void run() {
                            try (MapKeyContext<CharSequence> c = map2.context(key)) {
                                ReplicatedChronicleMap.ReplicatedContext rc =
                                        (ReplicatedChronicleMap.ReplicatedContext) c;
                                rc.newTimestamp = t;
                                rc.newIdentifier = (byte) 1;
                                c.remove();
                            }
                            task.decrementAndGet();
                        }
                    });

                } else {
                    try (MapKeyContext<CharSequence> c = map2.context(key)) {
                        ReplicatedChronicleMap.ReplicatedContext rc =
                                (ReplicatedChronicleMap.ReplicatedContext) c;
                        rc.newTimestamp = t;
                        rc.newIdentifier = (byte) 2;
                        c.remove();
                    }

                    task.incrementAndGet();
                    executorService.submit(new Runnable() {
                        @Override
                        public void run() {
                            try (MapKeyContext<CharSequence> c = map1.context(key)) {
                                ReplicatedChronicleMap.ReplicatedContext rc =
                                        (ReplicatedChronicleMap.ReplicatedContext) c;
                                rc.newTimestamp = t;
                                rc.newIdentifier = (byte) 2;
                                c.remove();
                            }
                            task.decrementAndGet();
                        }
                    });
                }
            }
        }

        while (task.get() > 0) {

        }
        Assert.assertEquals(new TreeMap(map1), new TreeMap(map2));
    }

    private void addRandomDelay(ExecutorService executorService, Random rnd) {
        if (rnd.nextInt(30) < 2) {

            task.incrementAndGet();
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    task.decrementAndGet();
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }




}

