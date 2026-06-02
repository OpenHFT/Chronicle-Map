/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.locks;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.map.ChronicleMap;
import org.junit.Assert;

import java.util.concurrent.CountDownLatch;

import static net.openhft.chronicle.values.Values.newNativeReference;

class WriterToo implements Runnable {
    private final CountDownLatch acquiredLatch;
    private final CountDownLatch releaseLatch;

    WriterToo() {
        this(null, null);
    }

    WriterToo(CountDownLatch acquiredLatch, CountDownLatch releaseLatch) {
        this.acquiredLatch = acquiredLatch;
        this.releaseLatch = releaseLatch;
    }

    @Override
    public void run() {
        try {
            ChronicleMap<String, BondVOInterface> chm =
                    DirtyReadTolerance.offHeap(
                            OS.getTarget() + "/shm-" +
                                    "OPERAND_CHRONICLE_MAP"
                    );
            System.out.println(
                    "WRITER TOO" +
                            " @t=" + System.currentTimeMillis() +
                            " DirtyReadOffender established chm "
            );
            ChronicleStampedLock offHeapLock = new ChronicleStampedLock(
                    OS.getTarget() + "/shm-"
                            + "OPERAND_ChronicleStampedLock"
            );
            Assert.assertNotEquals(null, offHeapLock);
            BondVOInterface bond = newNativeReference(BondVOInterface.class);
            chm.acquireUsing("369604101", bond);
            System.out.println(
                    "WRITER TOO" +
                            " @t=" + System.currentTimeMillis() +
                            " DirtyReadOffender ACQUIRING offHeapLock.writeLock();"
            );
            long stamp = 0;
            while ((stamp = offHeapLock.writeLock()) == 0) {
                Thread.yield();
            }
            DirtyReadTestSupport.signal(acquiredLatch);
            System.out.println(
                    "WRITER TOO" +
                            " @t=" + System.currentTimeMillis() +
                            " DirtyReadOffender ACQUIRED offHeapLock.writeLock();"
            );
            try {
                double newCoupon = 3.5 + Math.random();
                System.out.println(
                        "WRITER TOO" +
                                " @t=" + System.currentTimeMillis() +
                                " DirtyReadOffender " +
                                " calling chm.put('369604101'," + newCoupon + ") "
                );
                bond.setCoupon(newCoupon);
                chm.put("369604101", bond);
                System.out.println(
                        "WRITER TOO" +
                                " @t=" + System.currentTimeMillis() +
                                " DirtyReadOffender coupon=[" +
                                bond.getCoupon() +
                                "] written. "
                );
            } finally {
                System.out.println(
                        "WRITER TOO" +
                                " @t=" + System.currentTimeMillis() +
                                " DirtyReadOffender waiting up to " +
                                DirtyReadTestSupport.AWAIT_MILLIS + " ms"
                );
                DirtyReadTestSupport.awaitRelease(releaseLatch);
                offHeapLock.unlockWrite(stamp);
                System.out.println(
                        "WRITER TOO" +
                                " @t=" + System.currentTimeMillis() +
                                " DirtyReadOffender called " +
                                "offHeapLock.unlockWrite(" + stamp + ");"
                );
            }
            chm.close();
            offHeapLock.closeChronicle();
        } catch (Exception throwables) {
            throwables.printStackTrace();
        } finally {
            System.out.println(
                    "WRITER TOO" +
                            " ,,@t=" + System.currentTimeMillis() +
                            " DirtyReadOffender COMMITTED"
            );
        }
    }
}
