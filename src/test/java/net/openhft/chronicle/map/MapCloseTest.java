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
