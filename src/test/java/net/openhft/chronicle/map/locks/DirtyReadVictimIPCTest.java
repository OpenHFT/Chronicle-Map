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

public class DirtyReadVictimIPCTest {

    /**
     * ben.cotton@rutgers.edu -- should we even try to Test IPC compliance via this hack?
     * <p>
     * Conjecture:  Ben believes that: iff the operand set's members
     * are all Chronicle Off-Heap resouces
     * (e.g. Chronicle LongValue.class), then we may test IPC safety via an on-Heap j.l.Runnable
     * operator bridge.
     * <p>
     * this belief is STRICTLY conjecture.
     */

    @Test(timeout = 5_000)
    public void mainOptimisticNegative() throws Exception {
        DirtyReadTestSupport.prewarmGeneratedValueClasses();

        CountDownLatch writeComplete = new CountDownLatch(1);
        CountDownLatch releaseWriter = new CountDownLatch(1);
        Thread offendingWriter = new Thread(
                new DirtyReadOffenderIPCTest(writeComplete, releaseWriter),
                "dirty-offender-ipc"
        );

        ChronicleMap<String, BondVOInterface> chm =
                DirtyReadTolerance.offHeap(
                        OS.getTarget() + "/shm-OPERAND_CHRONICLE_MAP"
                );
        ChronicleStampedLock offHeapLock = new ChronicleStampedLock(
                OS.getTarget() + "/shm-" +
                        "OPERAND_ChronicleStampedLock"
        );
        try {
            BondVOInterface bond = newNativeReference(BondVOInterface.class);
            long stamp = offHeapLock.tryOptimisticRead();
            chm.acquireUsing("369604101", bond);
            bond = chm.get("369604101");
            Assert.assertNotNull(bond);

            offendingWriter.start();
            DirtyReadTestSupport.await(writeComplete, "IPC offender write");

            boolean valid = offHeapLock.validate(stamp);
            Assert.assertFalse("optimistic read should be invalidated by IPC writer", valid);
        } finally {
            releaseWriter.countDown();
            DirtyReadTestSupport.join(offendingWriter);
            chm.close();
            offHeapLock.closeChronicle();
        }
    }

    @Test(timeout = 2_000)
    public void mainOptimisticPositive() throws Exception {
        DirtyReadTestSupport.prewarmGeneratedValueClasses();

        ChronicleMap<String, BondVOInterface> chm =
                DirtyReadTolerance.offHeap(
                        OS.getTarget() + "/shm-OPERAND_CHRONICLE_MAP"
                );
        ChronicleStampedLock offHeapLock = new ChronicleStampedLock(
                OS.getTarget() + "/shm-"
                        + "OPERAND_ChronicleStampedLock"
        );
        try {
            BondVOInterface bond = newNativeReference(BondVOInterface.class);
            long stamp = offHeapLock.tryOptimisticRead();
            chm.acquireUsing("369604101", bond);
            bond = chm.get("369604101");
            Assert.assertNotNull(bond);

            Assert.assertTrue("optimistic read should remain valid without an IPC writer",
                    offHeapLock.validate(stamp));
        } finally {
            chm.close();
            offHeapLock.closeChronicle();
        }
    }
}
