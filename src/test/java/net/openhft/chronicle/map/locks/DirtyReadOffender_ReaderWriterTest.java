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

public class DirtyReadOffender_ReaderWriterTest {

    @Test(timeout = 5_000)
    public void main() throws Exception {
        DirtyReadTestSupport.prewarmGeneratedValueClasses();

        CountDownLatch readerAcquired = new CountDownLatch(1);
        CountDownLatch releaseReader = new CountDownLatch(1);
        Thread tooThread = new Thread(new ReaderToo(readerAcquired, releaseReader), "dirty-reader-too");

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
            DirtyReadTestSupport.await(readerAcquired, "reader helper acquired read lock");

            long stamp = offHeapLock.tryWriteLock();
            Assert.assertEquals("writer should be blocked by active reader", 0L, stamp);
        } finally {
            releaseReader.countDown();
            DirtyReadTestSupport.join(tooThread);
            chm.close();
            offHeapLock.closeChronicle();
        }
    }
}
