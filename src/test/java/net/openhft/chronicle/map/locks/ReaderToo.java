/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.locks;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.map.ChronicleMap;

import java.util.concurrent.CountDownLatch;

import static net.openhft.chronicle.values.Values.newNativeReference;

class ReaderToo implements Runnable {
    private final CountDownLatch readyLatch;
    private final CountDownLatch startLatch;
    private final CountDownLatch acquiredLatch;
    private final CountDownLatch releaseLatch;

    ReaderToo() {
        this(null, null);
    }

    ReaderToo(CountDownLatch acquiredLatch, CountDownLatch releaseLatch) {
        this(null, null, acquiredLatch, releaseLatch);
    }

    ReaderToo(CountDownLatch readyLatch,
              CountDownLatch startLatch,
              CountDownLatch acquiredLatch,
              CountDownLatch releaseLatch) {
        this.readyLatch = readyLatch;
        this.startLatch = startLatch;
        this.acquiredLatch = acquiredLatch;
        this.releaseLatch = releaseLatch;
    }

    @Override
    public void run() {
        try {
            ChronicleMap<String, BondVOInterface> chm =
                    DirtyReadTolerance.offHeap(
                            OS.getTarget() + "/shm-OPERAND_CHRONICLE_MAP"
                    );
            double coupon = 0.00;
            BondVOInterface bond = newNativeReference(BondVOInterface.class);
            System.out.println(
                    "READER_TOO " +
                            " ,,@t=" + System.currentTimeMillis() +
                            " DirtyReadIntolerant ENTERING offHeapLock.readLock()"
            );
            ChronicleStampedLock offHeapLock =
                    new ChronicleStampedLock(
                            OS.getTarget() + "/shm-"
                                    + "OPERAND_ChronicleStampedLock"
                    );
            DirtyReadTestSupport.signal(readyLatch);
            DirtyReadTestSupport.awaitIfPresent(startLatch, "reader start");
            long stamp = 0;
            while ((stamp = offHeapLock.tryReadLock()) == 0) {
                Thread.yield();
            }
            DirtyReadTestSupport.signal(acquiredLatch);
            System.out.println(
                    "READER_TOO " +
                            " ,,@t=" + System.currentTimeMillis() +
                            " DirtyReadIntolerant ENTERED offHeapLock.readLock() " +
                            " stamp=[" +
                            stamp +
                            "]"
            );
            try {
                chm.acquireUsing("369604101", bond);
                System.out.println(
                        "READER_TOO " +
                                " ,,@t=" + System.currentTimeMillis() +
                                " DirtyReadIntolerant calling chm.get('369604101').getCoupon()"
                );
                bond = chm.get("369604101");
                coupon = bond.getCoupon();
                System.out.println(
                        "READER_TOO " +
                                " ,,@t=" + System.currentTimeMillis() +
                                " DirtyReadIntolerant coupon=[" + coupon + "] read."
                );
                System.out.println(
                        "READER_TOO " +
                                " ,,@t=" + System.currentTimeMillis() +
                                " DirtyReadIntolerant waiting up to " +
                                DirtyReadTestSupport.AWAIT_MILLIS + " ms"
                );

                DirtyReadTestSupport.awaitRelease(releaseLatch);
                System.out.println(
                        "READER_TOO " +
                                " ,,@t=" + System.currentTimeMillis() +
                                " DirtyReadIntolerant awakening"
                );

            } finally {
                offHeapLock.unlockRead(stamp);
                System.out.println(
                        "READER_TOO " +
                                " ,,@t=" + System.currentTimeMillis() +
                                " DirtyReadIntolerant offHeapLock.unlockRead(" +
                                stamp + ") completed."
                );

            }
            System.out.println(
                    "READER_TOO " +
                            " ,,@t=" + System.currentTimeMillis() +
                            " DirtyReadIntolerant got() coupon=" +
                            coupon + " "
            );
            System.out.println(
                    "READER_TOO " +
                            " ,,@t=" + System.currentTimeMillis() +
                            " DirtyReadIntolerant COMMITTED"
            );
            chm.close();
            offHeapLock.closeChronicle();
        } catch (Exception throwables) {
            throwables.printStackTrace();
        }
    }
}
