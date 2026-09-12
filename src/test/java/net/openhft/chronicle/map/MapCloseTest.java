/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.ChronicleHashClosedException;
import net.openhft.chronicle.hash.impl.stage.hash.ChainingInterface;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

import static net.openhft.chronicle.hash.impl.BigSegmentHeader.LOCK_TIMEOUT_SECONDS;
import static net.openhft.chronicle.map.ChronicleMap.of;
import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings({"rawtypes", "unchecked"})
class MapCloseTest {

    @Test
    void closeInContextTest() {
        try (ChronicleMap<Integer, Integer> map = of(Integer.class, Integer.class).entries(1).create()) {
            try (ExternalMapQueryContext<Integer, Integer, ?> cxt = map.queryContext(1)) {
                cxt.readLock().lock();
                assertThrows(IllegalStateException.class, map::close);
                assertTrue(map.isOpen());
            }
            map.put(1, 2);
        }
    }

    @Test
    void testGetAfterCloseThrowsChronicleHashClosedException() throws InterruptedException {
        assertThrows(ChronicleHashClosedException.class, () -> {
            ChronicleMap<Integer, Integer> map =
                    of(Integer.class, Integer.class).entries(1).create();
            Thread t = new Thread(map::close);
            t.start();
            t.join();
            map.get(1);
        });
    }

    @Test
    void testIterationAfterCloseThrowsChronicleHashClosedException()
            throws InterruptedException {
        assertThrows(ChronicleHashClosedException.class, () -> {
            ChronicleMap<Integer, Integer> map =
                    of(Integer.class, Integer.class).entries(1).create();
            Thread t = new Thread(map::close);
            t.start();
            t.join();
            map.forEach((k, v) -> {
            });
        });
    }

    @Test
    void testSizeAfterCloseThrowsChronicleHashClosedException()
            throws InterruptedException {
        assertThrows(ChronicleHashClosedException.class, () -> {
            ChronicleMap<Integer, Integer> map =
                    of(Integer.class, Integer.class).entries(1).create();
            Thread t = new Thread(map::close);
            t.start();
            t.join();
            map.size();
        });
    }

    @Test
    void closeWithContextInAnotherThreadTest() throws InterruptedException {
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
    }

    @Test
    void testRemainingAutoResizesAfterClose() {
        assertThrows(ChronicleHashClosedException.class, () -> {
            ChronicleMap<Integer, Integer> map = of(Integer.class, Integer.class).entries(1).create();
            map.close();
            map.remainingAutoResizes();
        });
    }

    @Test
    void vanillaChronicleHashAllContextsExpungeTest() throws InterruptedException {
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
        assertEquals(2, map.allContexts().size());
        semaphore.release(2);
        t1.join();
        t2.join();

        map.get(1);
        assertEquals(1, map.allContexts().size());
        ChainingInterface cxt = map.allContexts().get(0).get().get();
        assertTrue(cxt == map.queryContext(1));
    }
}
