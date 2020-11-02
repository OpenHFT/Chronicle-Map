package net.openhft.chronicle.map.fromdocs.acid.revelations;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.fromdocs.BondVOInterface;

import java.util.concurrent.locks.StampedLock;

import static net.openhft.chronicle.values.Values.newNativeReference;

public class DirtyReadIntolerant {
    public static void main(String args[]) {
        try {
            String isoLevel = args[0];
            long sleepMock = Long.parseLong(args[1]);
            long holdTime = Long.parseLong(args[2]);
            /**
             *  ben.cotton@rutgers.edu   START
             */
            ChronicleMap<String, BondVOInterface> chm =
                    DirtyReadTolerance.offHeap(
                            args[3]
                                    + "OPERAND_CHRONICLE_MAP"
                    );
            Double coupon = 0.00;
            BondVOInterface bond = newNativeReference(BondVOInterface.class);
            //BondVOInterface cslMock = newNativeReference(BondVOInterface.class); //mock'd
            long stamp = 0;
            System.out.println(
                    " ,,@t=" + System.currentTimeMillis() +
                            " DirtyReadIntolerant ENTERING offHeapLock.readLock()"
            );
            StampedLock offHeapLock =
                    new ChronicleStampedLock(
                            args[3]
                                    + "OPERAND_ChronicleStampedLock"
                    );
            System.out.println(
                    " ,,@t=" + System.currentTimeMillis() +
                            " DirtyReadIntolerant sleeping " + sleepMock + " seconds"
            );
            Thread.sleep(sleepMock * 1_000);
            while ((stamp = offHeapLock.readLock()) < 0) {
                ;
            }
            System.out.println(
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
                        " ,,@t=" + System.currentTimeMillis() +
                                " DirtyReadIntolerant calling chm.get('369604101').getCoupon()"
                );
                bond = (BondVOInterface) chm.get("369604101");
                coupon = bond.getCoupon();
                System.out.println(
                        " ,,@t=" + System.currentTimeMillis() +
                                " DirtyReadIntolerant coupon=[" + coupon + "] read."
                );
                System.out.println(
                        " ,,@t=" + System.currentTimeMillis() +
                                " DirtyReadIntolerant sleeping " + holdTime + " seconds"
                );

                Thread.sleep(holdTime * 1_000);
                System.out.println(
                        " ,,@t=" + System.currentTimeMillis() +
                                " DirtyReadIntolerant awakening"
                );

            } finally {
                offHeapLock.unlockRead(stamp);
                System.out.println(
                        " ,,@t=" + System.currentTimeMillis() +
                                " DirtyReadIntolerant offHeapLock.unlockRead(" +
                                stamp + ") completed."
                );

            }
            /**
             *  ben.cotton@rutgers.edu   END
             */
            System.out.println(
                    " ,,@t=" + System.currentTimeMillis() +
                            " DirtyReadIntolerant got() coupon=" +
                            coupon + " "
            );
            System.out.println(
                    " ,,@t=" + System.currentTimeMillis() +
                            " DirtyReadIntolerant COMMITTED"
            );
        } catch (Exception throwables) {
            throwables.printStackTrace();
        }
    }
}
