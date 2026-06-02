/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.locks;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.map.ChronicleMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

import static net.openhft.chronicle.values.Values.newNativeReference;

public class DirtyReadOffender_WriterWriterTest {

    @Test(timeout = 5_000)
    public void main() throws Exception {
        DirtyReadTestSupport.prewarmGeneratedValueClasses();

        CountDownLatch writerAcquired = new CountDownLatch(1);
        CountDownLatch releaseWriter = new CountDownLatch(1);
        Thread tooThread = new Thread(new WriterToo(writerAcquired, releaseWriter), "dirty-writer-too");

        ChronicleMap<String, BondVOInterface> chm =
                DirtyReadTolerance.offHeap(
                        OS.getTarget() + "/shm-" +
                                "OPERAND_CHRONICLE_MAP"
                );
        ChronicleStampedLock offHeapLock = new ChronicleStampedLock(
                OS.getTarget() + "/shm-"
                        + "OPERAND_ChronicleStampedLock"
        );
        try {
            Assert.assertNotNull(offHeapLock);
            BondVOInterface bond = newNativeReference(BondVOInterface.class);
            chm.acquireUsing("369604101", bond);

            tooThread.start();
            DirtyReadTestSupport.await(writerAcquired, "writer helper acquired write lock");

            long stamp = offHeapLock.tryWriteLock();
            Assert.assertEquals("writer should be blocked by active writer", 0L, stamp);
        } finally {
            releaseWriter.countDown();
            DirtyReadTestSupport.join(tooThread);
            chm.close();
            offHeapLock.closeChronicle();
        }
    }
}
