package net.openhft.chronicle.map.locks;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.map.ChronicleMap;

import static net.openhft.chronicle.values.Values.newNativeReference;

class ReaderToo implements Runnable {

    @Override
    public void run() {
        try {
            String isoLevel = "READER_TOO";
            long sleepMock = Long.parseLong("0");
            long holdTime = Long.parseLong("20");
            /**
             *  ben.cotton@rutgers.edu   START
             */
            ChronicleMap<String, BondVOInterface> chm =
                    DirtyReadTolerance.offHeap(
                            OS.getTarget() + "/shm-OPERAND_CHRONICLE_MAP"
                    );
            Double coupon = 0.00;
            BondVOInterface bond = newNativeReference(BondVOInterface.class);
            //BondVOInterface cslMock = newNativeReference(BondVOInterface.class); //mock'd
            long stamp = 0;
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
            System.out.println(
                    "READER_TOO " +
                            " ,,@t=" + System.currentTimeMillis() +
                            " DirtyReadIntolerant sleeping " + sleepMock + " seconds"
            );
            Thread.sleep(sleepMock * 1_000);
            while ((stamp = offHeapLock.tryReadLock()) < 0) {
                //Assert.assertEquals(Boolean.TRUE, false); // we failed!;
            }
            //Assert.assertEquals(Boolean.TRUE, true); // we passed
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
                //chm.acquireUsing("Offender ", cslMock); //mock'd
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
                                " DirtyReadIntolerant sleeping " + holdTime + " seconds"
                );

                Thread.sleep(holdTime * 1_000);
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
            /**
             *  ben.cotton@rutgers.edu   END
             */

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
