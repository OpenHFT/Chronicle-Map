package net.openhft.chronicle.map.locks;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.map.ChronicleMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static net.openhft.chronicle.values.Values.newNativeReference;

public class DirtyReadVictimTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void mainOptimisticNegative() throws IOException {
        try {
            System.out.println("\n*****   Optimistic (-) Test\n");

            /**
             *  ben.cotton@rutgers.edu   START
             */

            ChronicleMap<String, BondVOInterface> chm =
                    DirtyReadTolerance.offHeap(
                            OS.getTarget() + "/shm-OPERAND_CHRONICLE_MAP"
                    );
            Double coupon = 0.00;
            BondVOInterface bond = newNativeReference(BondVOInterface.class);
            long stamp;
            System.out.println(
                    " ,,@t=" + System.currentTimeMillis() +
                            " DirtyReadVictim CALLING offHeapLock.tryOptimisticRead()"
            );
            ChronicleStampedLock offHeapLock = new ChronicleStampedLock(
                    OS.getTarget() + "/shm-" +
                            "OPERAND_ChronicleStampedLock"
            );
            while ((stamp = offHeapLock.tryOptimisticRead()) == 0) {
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
                                " DirtyReadVictim sleeping 20 seconds"
                );
                Thread offendingWriter = new Thread(
                        new DirtyReadOffenderTest()
                );
                offendingWriter.start();
                Thread.sleep(20_000);

            } finally {
                boolean r;
                if ((r = offHeapLock.validate(stamp))) {
                    System.out.println(
                            " ,,@t=" + System.currentTimeMillis() +
                                    " DirtyReadVictim OPTIMISTICALLY_READ coupon=" +
                                    coupon + " "
                    );
                    // THIS Test will/must FAIL. i.e. OPTIMISM tested (-) in this case
                    Assert.assertEquals(
                            Boolean.FALSE,
                            r
                    );
                } else {
                    System.out.println(
                            " ,,@t=" + System.currentTimeMillis() +
                                    " DirtyReadVictim FAILED offHeapLock.validate(stamp) " +
                                    " must apply PESSIMISTIC_POLICY (dirty read endured)" +
                                    " coupon=[" + coupon + "] is *DIRTY*. "
                    );
                    Assert.assertNotEquals(
                            Boolean.TRUE,
                            r
                    );
                }
                //offHeapLock.unlockWrite(writerStamp);
            }
            /**
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
            chm.close();
            offHeapLock.closeChronicle();
        } catch (Exception throwables) {
            throwables.printStackTrace();
        }
    }

    @Test
    public void mainOptimisticPositive() {
        System.out.println("\n*****   Optimistic (+) Test\n");
        try {
            /**
             *  ben.cotton@rutgers.edu   START
             */
            ChronicleMap<String, BondVOInterface> chm =
                    DirtyReadTolerance.offHeap(
                            OS.getTarget() + "/shm-OPERAND_CHRONICLE_MAP"
                    );
            Double coupon = 0.00;
            BondVOInterface bond = newNativeReference(BondVOInterface.class);
            long stamp = 0;
            System.out.println(
                    " ,,@t=" + System.currentTimeMillis() +
                            " DirtyReadVictim CALLING offHeapLock.tryOptimisticRead()"
            );
            ChronicleStampedLock offHeapLock = new ChronicleStampedLock(
                    OS.getTarget() + "/shm-"
                            + "OPERAND_ChronicleStampedLock"
            );
            while ((stamp = offHeapLock.tryOptimisticRead()) == 0) {
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
                Thread.sleep(2_000);
            } finally {
                if (offHeapLock.validate(stamp)) {
                    System.out.println(
                            " ,,@t=" + System.currentTimeMillis() +
                                    " DirtyReadVictim OPTIMISTICALLY_READ coupon=" +
                                    coupon + " "
                    );
                    // THIS Test will pass when ChronicleStampedLock is GA
                    Assert.assertEquals(
                            Boolean.TRUE,
                            true
                    );
                } else {
                    System.out.println(
                            " ,,@t=" + System.currentTimeMillis() +
                                    " DirtyReadVictim FAILED offHeapLock.validate(stamp) " +
                                    " must apply PESSIMISTIC_POLICY (dirty read endured)" +
                                    " coupon=[" + coupon + "] is *DIRTY*. "
                    );
                    // THIS Test will execute pass when ChronicleStampedLock is GA
                    Assert.assertNotEquals(
                            Boolean.TRUE,
                            false
                    );
                }
            }
            /**
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
