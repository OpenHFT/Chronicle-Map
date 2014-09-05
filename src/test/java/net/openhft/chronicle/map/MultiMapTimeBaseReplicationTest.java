/*
 * Copyright 2014 Higher Frequency Trading http://www.higherfrequencytrading.com
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

package net.openhft.chronicle.map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;

import static net.openhft.chronicle.map.Replica.ModificationNotifier.NOP;
import static org.junit.Assert.assertEquals;

/**
 * @author Rob Austin.
 */
public class MultiMapTimeBaseReplicationTest {

    private Replica.ModificationIterator segmentModificationIterator2;
    private Replica.ModificationIterator segmentModificationIterator1;
    private Builder.MapProvider<ReplicatedChronicleMap<Integer, Integer>> mapP1;
    private Builder.MapProvider<ReplicatedChronicleMap<Integer, Integer>> mapP2;
    private ReplicatedChronicleMap<Integer, Integer> map2;
    private ReplicatedChronicleMap<Integer, Integer> map1;


    private ArrayBlockingQueue<byte[]> map1ToMap2;
    private ArrayBlockingQueue<byte[]> map2ToMap1;

    @Before
    public void setup() throws IOException {
        final TimeProvider timeProvider = Mockito.mock(TimeProvider.class);

        map1ToMap2 = new ArrayBlockingQueue<byte[]>(10000);
        map2ToMap1 = new ArrayBlockingQueue<byte[]>(10000);

        mapP1 = Builder.newShmIntInt(20000, map2ToMap1, map1ToMap2, (byte) 1, (byte) 2);
        map1 = mapP1.getMap();
        segmentModificationIterator1 = map1.acquireModificationIterator((byte) 2, NOP);

        mapP2 = Builder.newShmIntInt(20000, map1ToMap2, map2ToMap1, (byte) 2, (byte) 1);
        map2 = mapP2.getMap();

        segmentModificationIterator2 = map2.acquireModificationIterator((byte) 1, NOP);

        Mockito.when(timeProvider.currentTimeMillis()).thenReturn((long) 1);
    }


    @Test
    public void testPut2Remove2Remove2Put1() throws IOException, InterruptedException {

        map2.put(1, 1, (byte) 2, 1399459457425L);
        map2.remove(1, null, (byte) 2, 1399459457425L);
        map2.remove(1, null, (byte) 2, 1399459457426L);
        //   waitTillFinished0();
        map1.put(1, 895, (byte) 1, 1399459457426L);


        // we will check 10 times that there all the work queues are empty
        waitTillEqual(5000);

        assertEquals(map1, map2);

    }


    @Test
    public void testPut1Remove2Remove2() throws IOException, InterruptedException {

        map1.put(1, 895, (byte) 1, 1399459457425L);
        waitTillFinished0();
        map2.remove(1, null, (byte) 2, 1399459457425L);
        map2.remove(1, null, (byte) 2, 1399459457426L);


        // we will check 10 times that there all the work queues are empty
        waitTillEqual(5000);
        assertEquals(map1, map2);

    }


    @Test
    public void testRemove1put2() throws IOException, InterruptedException {

        map2.remove(1, null, (byte) 2, 1399459457425L);
        waitTillFinished0();
        map1.put(1, 895, (byte) 1, 1399459457425L);

        // we will check 10 times that there all the work queues are empty
        waitTillEqual(5000);


        assertEquals(map1, map2);
    }


    @Test
    public void testRemove1put2Flip() throws IOException, InterruptedException {

        map1.remove(1, null, (byte) 1, 1399459457425L);
        waitTillFinished0();
        map2.put(1, 895, (byte) 2, 1399459457425L);

        // we will check 10 times that there all the work queues are empty
        waitTillEqual(5000);


        assertEquals(map1, map2);
    }


    @Test
    public void testPut1Put1CrazyTimes() throws IOException, InterruptedException {

        map1.put(1, 894, (byte) 1, 1399459457425L);
        waitTillFinished0();
        map2.put(1, 895, (byte) 2, 0L);

        // we will check 10 times that there all the work queues are empty
        waitTillEqual(5000);


        assertEquals(map1, map2);
    }


    @Test
    public void testPut1Put1SameTimes() throws IOException, InterruptedException {

        map1.put(1, 10, (byte) 1, 0L);
        waitTillFinished0();
        map2.put(1, 20, (byte) 2, 0L);

        // we will check 10 times that there all the work queues are empty
        waitTillEqual(5000);


        assertEquals(map1, map2);
    }

    @Test
    public void testPut1Put1SameTimesFlip() throws IOException, InterruptedException {

        map2.put(1, 894, (byte) 2, 0L);
        waitTillFinished0();
        map1.put(1, 895, (byte) 1, 0L);

        // we will check 10 times that there all the work queues are empty
        waitTillEqual(5000);


        assertEquals(map1, map2);
    }

    @Test
    public void testPut1Put1CrazyTimesMapFlip() throws IOException, InterruptedException {

        map2.put(1, 894, (byte) 2, 1399459457425L);
        waitTillFinished0();
        map1.put(1, 895, (byte) 1, 0L);

        // we will check 10 times that there all the work queues are empty
        waitTillEqual(5000);


        assertEquals(map1, map2);
    }


    @Test
    public void testRemovePut() throws IOException, InterruptedException {


        map2.remove(1, null, (byte) 2, 1399459457425L);
        waitTillFinished0();
        map1.put(1, 1, (byte) 1, 1399459457425L);

        waitTillEqual(5000);

        assertEquals(map1, map2);

    }


    @Test
    public void testRemovePutFlip() throws IOException, InterruptedException {


        map1.remove(1, null, (byte) 1, 1399459457425L);
        waitTillFinished0();
        map2.put(1, 1, (byte) 2, 1399459457425L);
        waitTillEqual(5000);
        // we will check 10 times that there all the work queues are empty

        assertEquals(map1, map2);


    }


    @Test
    public void testPutRemove() throws IOException, InterruptedException {


        map2.put(1, 1, (byte) 2, 1399459457425L);
        waitTillFinished0();
        map1.remove(1, null, (byte) 1, 1399459457425L);
        waitTillEqual(5000);
        // we will check 10 times that there all the work queues are empty

        assertEquals(map1, map2);

    }


    @Test
    public void testPutRemoveFilp() throws IOException, InterruptedException {


        map1.put(1, 1, (byte) 1, 1399459457425L);
        waitTillFinished0();
        map2.remove(1, null, (byte) 2, 1399459457425L);
        waitTillEqual(5000);
        // we will check 10 times that there all the work queues are empty

        assertEquals(map1, map2);

    }


    private void waitTillFinished0() throws InterruptedException {
        int i = 0;
        for (; i < 2; i++) {
            if (!(map2ToMap1.isEmpty() && map2ToMap1.isEmpty() &&
                    !segmentModificationIterator1.hasNext() &&
                    !segmentModificationIterator2.hasNext() &&
                    mapP1.isQueueEmpty() && mapP2.isQueueEmpty())) {
                i = 0;
            }
            Thread.sleep(1);
        }
    }

    /**
     * waits until map1 and map2 show the same value
     *
     * @param timeOutMs timeout in milliseconds
     * @throws InterruptedException
     */
    private void waitTillEqual(final int timeOutMs) throws InterruptedException {
        int t = 0;
        for (; t < timeOutMs; t++) {
            waitTillFinished0();
            if (map1.equals(map2))
                break;
            Thread.sleep(1);
        }
    }


}
