package net.openhft.chronicle.map.fromdocs.acid.revelations;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.fromdocs.BondVOInterface;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static net.openhft.chronicle.values.Values.newNativeReference;
import static org.junit.Assert.*;

public class DirtyReadVictimTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
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
/*                            "C:\\Users\\buddy\\dev\\shm\\OPERAND_CHRONICLE_MAP"*/
                    );
            Double coupon = 0.00;
            BondVOInterface bond = newNativeReference(BondVOInterface.class);
            long stamp = 0;
            System.out.println(
                    " ,,@t=" + System.currentTimeMillis() +
                            " DirtyReadVictim CALLING offHeapLock.tryOptimisticRead()"
            );
            ChronicleStampedLock offHeapLock = new ChronicleStampedLock(
                    OS.getTarget() + "/OPERAND_ChronicleStampedLock"
                    /*"C:\\Users\\buddy\\dev\\shm\\OPERAND_ChronicleStampedLock"*/
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
                            offHeapLock.chmW.get("WriterCount ").getVolatileValue(),
                            0L
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