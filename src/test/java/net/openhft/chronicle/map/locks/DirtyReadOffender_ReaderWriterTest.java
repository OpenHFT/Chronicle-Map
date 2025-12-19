/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.locks;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.map.ChronicleMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.values.Values.newNativeReference;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

public class DirtyReadOffender_ReaderWriterTest {

    @BeforeEach
    public void longRunningStableOnLinux() {
        assumeFalse(OS.isLinux());
    }

    @Timeout(value = 60_000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void main() {
        try {
            final long sleepT = Long.parseLong("8");
            long holdTime = Long.parseLong("20");

            Thread tooThread = new Thread(new ReaderToo());
            tooThread.start();

            ChronicleMap<String, BondVOInterface> chm =
                    DirtyReadTolerance.offHeap(
                            OS.getTarget() + "/shm-" +
                                    "OPERAND_CHRONICLE_MAP"
                    );
            System.out.println(
                    "                             " +
                            " @t=" + System.currentTimeMillis() +
                            " DirtyReadOffender established chm "
            );
            ChronicleStampedLock offHeapLock = new ChronicleStampedLock(
                    OS.getTarget() + "/shm-"
                            + "OPERAND_ChronicleStampedLock"
            );
            Assertions.assertNotEquals(null, offHeapLock);
            BondVOInterface bond = newNativeReference(BondVOInterface.class);
            //BondVOInterface cslMock = newNativeReference(BondVOInterface.class);
            chm.acquireUsing("369604101", bond);
            //chm.acquireUsing("Offender ", cslMock); // mock ChronicleStampLock
            System.out.println(
                    "                             " +
                            " @t=" + System.currentTimeMillis() +
                            " DirtyReadOffender sleeping " + sleepT + " seconds "
            );
            Thread.sleep(sleepT * 1_000);
            System.out.println(
                    "                             " +
                            " @t=" + System.currentTimeMillis() +
                            " DirtyReadOffender awakening "
            );
            /*
               ben.cotton@rutgers.edu  ... anticipate Chronicle (www.OpenHFT.net)
               providing a j.u.c.l.StampedLock API for off-heap enthusiasts
              <p>
               START

             */
            long stamp;
            System.out.println(
                    "                             " +
                            " @t=" + System.currentTimeMillis() +
                            " DirtyReadOffender ACQUIRING offHeapLock.tryWriteLock();"
            );
            int blockedByHoldingReaderCount = 0;
            while ((stamp = offHeapLock.tryWriteLock()) == 0) { //Ben?
                Thread.sleep(1_000);
                ++blockedByHoldingReaderCount;
                System.out.println(
                        "                             " +
                                " @t=" + System.currentTimeMillis() +
                                " WAITING in offHeapLock.tryWriteLock()... "
                );

            }
            Assertions.assertNotEquals(0, blockedByHoldingReaderCount);
            System.out.println(
                    "                             " +
                            " @t=" + System.currentTimeMillis() +
                            " DirtyReadOffender ACQUIRED offHeapLock.tryWriteLock();"
            );
            try {
                double newCoupon = 3.5 + Math.random();
                System.out.println(
                        "                             " +
                                " @t=" + System.currentTimeMillis() +
                                " DirtyReadOffender " +
                                " calling chm.put('369604101'," + newCoupon + ") "
                );
                bond.setCoupon(newCoupon);
                chm.put("369604101", bond);
                //cslMock.setEntryLockState(System.currentTimeMillis()); //mock'd
                // chm.put("Offender ",cslMock); //mock'd
                System.out.println(
                        "                             " +
                                " @t=" + System.currentTimeMillis() +
                                " DirtyReadOffender coupon=[" +
                                bond.getCoupon() +
                                "] written. "
                );
            } finally {
                System.out.println(
                        "                             " +
                                " @t=" + System.currentTimeMillis() +
                                " DirtyReadOffender sleeping " + holdTime + " seconds "
                );
                Thread.sleep(holdTime * 1_000);
                offHeapLock.unlockWrite(stamp);
                System.out.println(
                        "                             " +
                                " @t=" + System.currentTimeMillis() +
                                " DirtyReadOffender called " +
                                "offHeapLock.unlockWrite(" + stamp + ");"
                );
            }
            /*
               ben.cotton@rutgers.edu
              <p>
               END

             */
            chm.close();
            offHeapLock.closeChronicle();
        } catch (Exception throwables) {
            throwables.printStackTrace();
        } finally {
            System.out.println(
                    "                             " +
                            " ,,@t=" + System.currentTimeMillis() +
                            " DirtyReadOffender COMMITTED"
            );
        }
    }
}
