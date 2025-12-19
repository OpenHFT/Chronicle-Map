/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.ChronicleHashClosedException;
import net.openhft.chronicle.hash.impl.stage.hash.ChainingInterface;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

import static net.openhft.chronicle.hash.impl.BigSegmentHeader.LOCK_TIMEOUT_SECONDS;
import static net.openhft.chronicle.map.ChronicleMap.of;

@SuppressWarnings({"rawtypes", "unchecked"})
public class MapCloseTest {

    @Test
    public void closeInContextTest() {
        ChronicleMap<Integer, Integer> map =
                of(Integer.class, Integer.class).entries(1).create();
        ExternalMapQueryContext<Integer, Integer, ?> cxt = map.queryContext(1);
        map.close();
        Assertions.assertThrows(ChronicleHashClosedException.class, () -> map.get(1),
                "map.get after close should throw");
    }

    @Test
    public void testGetAfterCloseThrowsChronicleHashClosedException() throws InterruptedException {
        ChronicleMap<Integer, Integer> map =
                of(Integer.class, Integer.class).entries(1).create();
        Thread t = new Thread(map::close);
        t.start();
        t.join();
        Assertions.assertThrows(ChronicleHashClosedException.class, () -> map.get(1));
    }

    @Test
    public void testIterationAfterCloseThrowsChronicleHashClosedException()
            throws InterruptedException {
        ChronicleMap<Integer, Integer> map =
                of(Integer.class, Integer.class).entries(1).create();
        Thread t = new Thread(map::close);
        t.start();
        t.join();
        Assertions.assertThrows(ChronicleHashClosedException.class, () -> map.forEach((k, v) -> {
        }));
    }

    @Test
    public void testSizeAfterCloseThrowsChronicleHashClosedException()
            throws InterruptedException {
        ChronicleMap<Integer, Integer> map =
                of(Integer.class, Integer.class).entries(1).create();
        Thread t = new Thread(map::close);
        t.start();
        t.join();
        Assertions.assertThrows(ChronicleHashClosedException.class, map::size);
    }

    @Test
    public void closeWithContextInAnotherThreadTest() throws InterruptedException {
        LOCK_TIMEOUT_SECONDS = 2;
        ChronicleMap<Integer, Integer> map =
                of(Integer.class, Integer.class).entries(1).create();
        Object lock = new Object();
        CountDownLatch latch = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        synchronized (lock) {
            new Thread() {
                @Override
                public void run() {
                    ExternalMapQueryContext<Integer, Integer, ?> cxt = map.queryContext(1);
                    latch.countDown();
                    synchronized (lock) {
                        cxt.close();
                        latch2.countDown();
                    }
                }
            }.start();
            latch.await();
            map.close();
        }
        latch2.await();
        map.close();
        LOCK_TIMEOUT_SECONDS = 60;
        Assertions.assertEquals(60, LOCK_TIMEOUT_SECONDS, "lock timeout should be restored to default value after test");
    }

    @Test
    public void testRemainingAutoResizesAfterClose() {
        ChronicleMap<Integer, Integer> map = of(Integer.class, Integer.class).entries(1).create();
        map.close();
        Assertions.assertThrows(ChronicleHashClosedException.class, map::remainingAutoResizes);
    }

    @Test
    public void vanillaChronicleHashAllContextsExpungeTest() throws InterruptedException {
        VanillaChronicleMap<Integer, Integer, Void> map =
                (VanillaChronicleMap<Integer, Integer, Void>)
                        of(Integer.class, Integer.class).entries(1).create();
        Semaphore semaphore = new Semaphore(0);
        CountDownLatch latch = new CountDownLatch(2);
        class MapAccessThread extends Thread {
            @Override
            public void run() {
                map.get(1);
                latch.countDown();
                try {
                    semaphore.acquire();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        MapAccessThread t1 = new MapAccessThread();
        MapAccessThread t2 = new MapAccessThread();
        t1.start();
        t2.start();
        latch.await();
        Assertions.assertEquals(2, map.allContexts().size(), "context pool should contain one context per active thread");
        semaphore.release(2);
        t1.join();
        t2.join();

        map.get(1);
        Assertions.assertEquals(1, map.allContexts().size(), "context pool should be reduced to single context after threads complete");
        ChainingInterface cxt = map.allContexts().get(0).get().get();
        Assertions.assertSame(cxt, map.queryContext(1), "query context should reuse the pooled context instance");
    }
}
