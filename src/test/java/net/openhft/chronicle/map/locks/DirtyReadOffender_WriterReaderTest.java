package net.openhft.chronicle.map.locks;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.map.ChronicleMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static net.openhft.chronicle.values.Values.newNativeReference;

public class DirtyReadOffender_WriterReaderTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void main() {
        try {
            long sleepT = Long.parseLong("8");
            long holdTime = Long.parseLong("20");

            Thread tooThread = new Thread(new WriterToo());
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
            Assert.assertNotEquals(offHeapLock, null);
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
            /**
             *  ben.cotton@rutgers.edu  ... anticipate Chronicle (www.OpenHFT.net)
             *  providing a j.u.c.l.StampedLock API for off-heap enthusiasts
             *
             *  START
             *
             */
            long stamp = 0;
            System.out.println(
                    "                             " +
                            " @t=" + System.currentTimeMillis() +
                            " DirtyReadOffender ACQUIRING offHeapLock.tryReadLock();"
            );
            int blockedByHoldingWriterCount = 0;
            while ((stamp = offHeapLock.tryReadLock()) == 0) {
                Thread.sleep(1_000);
                ++blockedByHoldingWriterCount;
                System.out.println(
                        "                             " +
                                " @t=" + System.currentTimeMillis() +
                                " WAITING in offHeapLock.tryReadLock()... "
                );

            }
            Assert.assertNotEquals(blockedByHoldingWriterCount, 0);
            System.out.println(
                    "                             " +
                            " @t=" + System.currentTimeMillis() +
                            " DirtyReadOffender ACQUIRED offHeapLock.tryReadLock();"
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
                offHeapLock.unlockRead(stamp);
                System.out.println(
                        "                             " +
                                " @t=" + System.currentTimeMillis() +
                                " DirtyReadOffender called " +
                                "offHeapLock.unlockWrite(" + stamp + ");"
                );
            }
            /**
             *  ben.cotton@rutgers.edu
             *
             *  END
             *
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
