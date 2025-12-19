/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.fromdocs.acid.revelations;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.fromdocs.BondVOInterface;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static net.openhft.chronicle.values.Values.newNativeReference;

@SuppressWarnings({"rawtypes", "unchecked"})
public class DirtyReadVictimTest {

    @BeforeEach
    public void setUp() {
    }

    @AfterEach
    public void tearDown() {
    }

    @Test
    public void main() {
        try {
            /*
             *  ben.cotton@rutgers.edu   START
             */
            ChronicleMap<String, BondVOInterface> chm =
                    DirtyReadTolerance.offHeap(
                            OS.getTarget() + "/OPERAND_CHRONICLE_MAP"
                    );
            double coupon = 0.00;
            BondVOInterface bond = newNativeReference(BondVOInterface.class);
            long stamp;
            System.out.println(
                    " ,,@t=" + System.currentTimeMillis() +
                            " DirtyReadVictim CALLING offHeapLock.tryOptimisticRead()"
            );
            ChronicleStampedLock offHeapLock = new ChronicleStampedLock(
                    OS.getTarget() + "/OPERAND_ChronicleStampedLock"
            );
            while ((stamp = offHeapLock.tryOptimisticRead()) == 0) {
                // none
            }
            System.out.println(
                    " ,,@t=" + System.currentTimeMillis() +
                            " DirtyReadVictim CALLED offHeapLock.tryOptimisticRead()"
            );
            try {
                chm.acquireUsing("369604101", bond);
                System.out.println(
                        " ,,@t=" + System.currentTimeMillis() +
                                " DirtyReadVictim calling chm.get('369604101').getCoupon()"
                );
                bond = chm.get("369604101");
                coupon = bond.getCoupon();
                System.out.println(
                        " ,,@t=" + System.currentTimeMillis() +
                                " DirtyReadVictim coupon=[" + coupon + "] read."
                );
                System.out.println(
                        " ,,@t=" + System.currentTimeMillis() +
                                " DirtyReadVictim sleeping 2 seconds"
                );
                Thread.sleep(500);

            } finally {
                if (offHeapLock.validate(stamp)) {
                    System.out.println(
                            " ,,@t=" + System.currentTimeMillis() +
                                    " DirtyReadVictim OPTIMISTICALLY_READ coupon=" +
                                    coupon + " "
                    );
                    // THIS Test will pass when ChronicleStampedLock is GA
                    Assertions.assertEquals(
                            0L,
                            offHeapLock.chmW.get("WriterCount ").getVolatileValue()
                    , "offHeapLock.chmW.get(<str>).getVolatileValue()");
                } else {
                    System.out.println(
                            " ,,@t=" + System.currentTimeMillis() +
                                    " DirtyReadVictim FAILED offHeapLock.validate(stamp) " +
                                    " must apply PESSIMISTIC_POLICY (dirty read endured)" +
                                    " coupon=[" + coupon + "] is *DIRTY*. "
                    );
                    // THIS Test will execute pass when ChronicleStampedLock is GA
                    Assertions.assertNotEquals(
                            stamp,
                            offHeapLock.lastWriterT.getEntryLockState()
                    );
                }
            }
            /*
             *  ben.cotton@rutgers.edu   END
             */
            System.out.println(
                    " ,,@t=" + System.currentTimeMillis() +
                            " DirtyReadVictim got() coupon=" +
                            coupon + " "
            );
            System.out.println(
                    " ,,@t=" + System.currentTimeMillis() +
                            " DirtyReadVictim COMMITTED"
            );
        } catch (Exception throwables) {
            throwables.printStackTrace();
        }
    }
}
