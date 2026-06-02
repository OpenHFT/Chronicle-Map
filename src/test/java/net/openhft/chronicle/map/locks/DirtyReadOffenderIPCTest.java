/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.locks;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.map.ChronicleMap;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.StampedLock;

import static net.openhft.chronicle.values.Values.newNativeReference;

public class DirtyReadOffenderIPCTest implements Runnable {
    private final CountDownLatch writeCompleteLatch;
    private final CountDownLatch releaseLatch;

    public DirtyReadOffenderIPCTest() {
        this(null, null);
    }

    DirtyReadOffenderIPCTest(CountDownLatch writeCompleteLatch, CountDownLatch releaseLatch) {
        this.writeCompleteLatch = writeCompleteLatch;
        this.releaseLatch = releaseLatch;
    }

    @Test
    public void run() {
        try {
            ChronicleMap<String, BondVOInterface> chm =
                    DirtyReadTolerance.offHeap(
                            OS.getTarget() + "/shm-"
                                    + "OPERAND_CHRONICLE_MAP"
                    );
            System.out.println(
                    "..... @t=" + System.currentTimeMillis() +
                            " DirtyReadOffender established chm "
            );
            final StampedLock offHeapLock = new ChronicleStampedLock(
                    OS.getTarget() + "/shm-"
                            + "OPERAND_ChronicleStampedLock"
            );
            BondVOInterface bond = newNativeReference(BondVOInterface.class);

            chm.acquireUsing("369604101", bond);
            long stamp = 0;
            System.out.println(
                    "..... @t=" + System.currentTimeMillis() +
                            " DirtyReadOffender ACQUIRING offHeapLock.writeLock();"
            );
            while ((stamp = offHeapLock.writeLock()) == 0) {
                // none
            }
            System.out.println(
                    "..... @t=" + System.currentTimeMillis() +
                            " DirtyReadOffender ACQUIRED offHeapLock.writeLock();"
            );
            try {
                double newCoupon = 3.5 + Math.random();
                System.out.println(
                        "..... @t=" + System.currentTimeMillis() +
                                " DirtyReadOffender " +
                                " calling chm.put('369604101'," + newCoupon + ") "
                );
                bond.setCoupon(newCoupon);
                chm.put("369604101", bond);
                DirtyReadTestSupport.signal(writeCompleteLatch);
                System.out.println(
                        "..... @t=" + System.currentTimeMillis() +
                                " DirtyReadOffender coupon=[" +
                                bond.getCoupon() +
                                "] written. "
                );
            } finally {
                System.out.println(
                        "..... @t=" + System.currentTimeMillis() +
                                " DirtyReadOffender waiting up to " +
                                DirtyReadTestSupport.AWAIT_MILLIS + " ms"
                );
                DirtyReadTestSupport.awaitRelease(releaseLatch);
                offHeapLock.unlockWrite(stamp);
                System.out.println(
                        "..... @t=" + System.currentTimeMillis() +
                                " DirtyReadOffender called " +
                                "offHeapLock.unlockWrite(" + stamp + ");"
                );
            }
        } catch (Exception throwables) {
            throwables.printStackTrace();
        } finally {
            System.out.println(
                    "..... @t=" + System.currentTimeMillis() +
                            " DirtyReadOffender COMMITTED"
            );
        }
    }
}
