package net.openhft.chronicle.map.fromdocs.acid.revelations;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.fromdocs.BondVOInterface;
import net.openhft.chronicle.map.fromdocs.acid.ChronicleAcidIsolation;
import static net.openhft.chronicle.values.Values.newNativeReference;

public class DirtyReadVictim {

    public static void main(String args[]) {
        try {
            String isoLevel = args[0];
            long sleepMock = Long.parseLong(args[1]);

            /**
             *  ben.cotton@rutgers.edu   START
             */
            ChronicleMap<String, BondVOInterface> chm =
                    DirtyReadTolerance.offHeap(
                            "C:\\Users\\buddy\\dev\\shm\\OPERAND_CHRONICLE_MAP"
                    );
            Double coupon = 0.00;
            BondVOInterface bond = newNativeReference(BondVOInterface.class);
            BondVOInterface cslMock = newNativeReference(BondVOInterface.class); //mock'd
            long stamp = 0;
            System.out.println(
                    " ,,@t=" + System.currentTimeMillis() +
                            " DirtyReadVictim ENTERING offHeapLock.tryOptimisticRead()"
            );
            ChronicleStampedLock offHeapLock = new ChronicleStampedLock();
            while ((stamp = offHeapLock.tryOptimisticRead()) == 0) {
                ;
            }
            System.out.println(
                    " ,,@t=" + System.currentTimeMillis() +
                            " DirtyReadVictim ENTERED offHeapLock.tryOptimisticRead()"
            );
            try {
                chm.acquireUsing("369604101", bond);
                chm.acquireUsing("Offender ", cslMock); //mock'd
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
                                " DirtyReadVictim sleeping "+sleepMock+" seconds"
                );
                cslMock.setEntryLockState(0);
                chm.put("Offender ", cslMock); //mock'd
                Thread.sleep(sleepMock * 1_000);
                System.out.println(
                        " ,,@t=" + System.currentTimeMillis() +
                                " DirtyReadVictim awakening"
                );
                //cslMock = (BondVOInterface) chm.get("Offender "); //mock'd
                cslMock = (BondVOInterface) chm.get("Offender "); //mock'd

            } finally {
                if (offHeapLock.validate(stamp) && (cslMock.getEntryLockState() == 0)) {
                    System.out.println(
                            " ,,@t=" + System.currentTimeMillis() +
                                    " DirtyReadVictim OPTIMISTICALLY_READ coupon=" +
                                    coupon + " "
                    );

                } else {
                    System.out.println(
                            " ,,@t=" + System.currentTimeMillis() +
                                    " DirtyReadVictim FAILED offHeapLock.validate(stamp) " +
                                    " must apply PESSIMISTIC_POLICY (dirty read endured)" +
                                    " coupon=[" + coupon + "] is *DIRTY*. "
                    );
                }
                System.out.println(
                        " ,,@t=" + System.currentTimeMillis() +
                                " DirtyReadVictim EXITED offHeapLock.tryOptimisticRead()"
                );
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

