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

/**
 * This Test efforts to ensure that a READERS-only set of requests to access the CSL
 * is always granted (i.e. csl.tryReadLock()  ALWAYS returns true)
 */

public class DirtyReadIntolerant_ReaderReader_Test {

    @Test(timeout = 5_000)
    public void main() throws Exception {
        DirtyReadTestSupport.prewarmGeneratedValueClasses();

        CountDownLatch readerReady = new CountDownLatch(1);
        CountDownLatch startReaders = new CountDownLatch(1);
        CountDownLatch readerAcquired = new CountDownLatch(1);
        CountDownLatch releaseReader = new CountDownLatch(1);
        Thread tooThread = new Thread(
                new ReaderToo(readerReady, startReaders, readerAcquired, releaseReader),
                "dirty-reader-too"
        );

        ChronicleMap<String, BondVOInterface> chm =
                DirtyReadTolerance.offHeap(
                        OS.getTarget() + "/shm-OPERAND_CHRONICLE_MAP"
                );
        ChronicleStampedLock offHeapLock =
                new ChronicleStampedLock(
                        OS.getTarget() + "/shm-"
                                + "OPERAND_ChronicleStampedLock"
                );
        long stamp = 0;
        try {
            BondVOInterface bond = newNativeReference(BondVOInterface.class);
            chm.acquireUsing("369604101", bond);

            tooThread.start();
            DirtyReadTestSupport.await(readerReady, "reader helper ready");
            startReaders.countDown();

            stamp = offHeapLock.tryReadLock();
            Assert.assertTrue("reader should acquire while another reader is active", stamp > 0);
            DirtyReadTestSupport.await(readerAcquired, "reader helper acquired read lock");

            bond = chm.get("369604101");
            Assert.assertNotNull(bond);
            System.out.println(
                    "                             " +
                            " ,,@t=" + System.currentTimeMillis() +
                            " DirtyReadIntolerant coupon=[" + bond.getCoupon() + "] read."
            );
        } finally {
            startReaders.countDown();
            if (stamp > 0) {
                offHeapLock.unlockRead(stamp);
            }
            releaseReader.countDown();
            DirtyReadTestSupport.join(tooThread);
            chm.close();
            offHeapLock.closeChronicle();
        }
    }
}
