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

import net.openhft.chronicle.hash.ChronicleHashClosedException;
import net.openhft.chronicle.hash.impl.stage.hash.ChainingInterface;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

import static net.openhft.chronicle.hash.impl.BigSegmentHeader.LOCK_TIMEOUT_SECONDS;

public class MapCloseTest {

    @Test(expected = IllegalStateException.class)
    public void closeInContextTest() {
        ChronicleMap<Integer, Integer> map =
                ChronicleMap.of(Integer.class, Integer.class).entries(1).create();
        ExternalMapQueryContext<Integer, Integer, ?> cxt = map.queryContext(1);
        map.close();
    }

    @Test(expected = ChronicleHashClosedException.class)
    public void testGetAfterCloseThrowsChronicleHashClosedException() throws InterruptedException {
        ChronicleMap<Integer, Integer> map =
                ChronicleMap.of(Integer.class, Integer.class).entries(1).create();
        Thread t = new Thread(() -> map.close());
        t.start();
        t.join();
        map.get(1);
    }

    @Test(expected = ChronicleHashClosedException.class)
    public void testIterationAfterCloseThrowsChronicleHashClosedException()
            throws InterruptedException {
        ChronicleMap<Integer, Integer> map =
                ChronicleMap.of(Integer.class, Integer.class).entries(1).create();
        Thread t = new Thread(() -> map.close());
        t.start();
        t.join();
        map.forEach((k, v) -> {
        });
    }

    @Test(expected = ChronicleHashClosedException.class)
    public void testSizeAfterCloseThrowsChronicleHashClosedException()
            throws InterruptedException {
        ChronicleMap<Integer, Integer> map =
                ChronicleMap.of(Integer.class, Integer.class).entries(1).create();
        Thread t = new Thread(() -> map.close());
        t.start();
        t.join();
        map.size();
    }

    @Test(expected = RuntimeException.class)
    public void closeWithContextInAnotherThreadTest() throws InterruptedException {
        LOCK_TIMEOUT_SECONDS = 2;
        ChronicleMap<Integer, Integer> map =
                ChronicleMap.of(Integer.class, Integer.class).entries(1).create();
        Object lock = new Object();
        CountDownLatch latch = new CountDownLatch(1);
        synchronized (lock) {
            new Thread() {
                @Override
                public void run() {
                    ExternalMapQueryContext<Integer, Integer, ?> cxt = map.queryContext(1);
                    latch.countDown();
                    synchronized (lock) {
                        cxt.close();
                    }
                }
            }.start();
            latch.await();
            map.close();
        }
        LOCK_TIMEOUT_SECONDS = 60;
    }

    @Test
    public void vanillaChronicleHashAllContextsExpungeTest() throws InterruptedException {
        VanillaChronicleMap<Integer, Integer, Void> map =
                (VanillaChronicleMap<Integer, Integer, Void>)
                        ChronicleMap.of(Integer.class, Integer.class).entries(1).create();
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
        Assert.assertEquals(2, map.allContexts().size());
        semaphore.release(2);
        t1.join();
        t2.join();

        map.get(1);
        Assert.assertEquals(1, map.allContexts().size());
        ChainingInterface cxt = map.allContexts().get(0).get().get();
        Assert.assertTrue(cxt == map.queryContext(1));
    }
}
