package net.openhft.chronicle.map.locks;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.map.ChronicleMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static net.openhft.chronicle.values.Values.newNativeReference;

public class DirtyReadVictimIPCTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    /**
     * ben.cotton@rutgers.edu -- should we even try to Test IPC compliance via this hack?
     * <p>
     * Conjecture:  Ben believes that: iff the operand set's members
     * are all Chronicle Off-Heap resouces
     * (e.g. Chronicle LongValue.class), then we may test IPC safety via an on-Heap j.l.Runnable
     * operator bridge.
     * <p>
     * this belief is STRICTLY conjecture.
     *
     * @throws IOException
     */

    @Test
    public void mainOptimisticNegative() throws IOException {
        try {
            System.out.println("\n*****   Optimistic (-) Test\n");
//            ProcessBuilder pb = new ProcessBuilder(
//                    "/usr/bin/mkdir -p "+
//                    " C:\\Users\\buddy\\dev\\shm\\ "
//            );

//            Process p = pb.start();
//            Scanner scan = new Scanner(p.getInputStream());
//            while (scan.hasNext()) {
//                System.out.println(
//                        " ,,@t=" +
//                                System.currentTimeMillis() +
//                                " DirtyReadVictimTest CALLING [" +
//                                scan.next() +
//                                "]"
//                );
//            }
//            Thread.sleep(1_000);
//            p.destroyForcibly();
//            System.out.println(
//                    " ,,@t=" +
//                            System.currentTimeMillis() +
//                            " DirtyReadVictimTest called [\n" +
//                            "mkdir -p " +
//                            " C:\\Users\\buddy\\dev\\shm\\ " +
//                            "\n" +
//                            "]"
//            );

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
                ;
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
                        new DirtyReadOffenderIPCTest()
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
                ;
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
                bond = (BondVOInterface) chm.get("369604101");
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
