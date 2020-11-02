package net.openhft.chronicle.map.fromdocs.acid.exodus;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.fromdocs.BondVOInterface;
import net.openhft.chronicle.map.fromdocs.acid.ChronicleAcidIsolation;

import java.util.Scanner;
import java.util.concurrent.locks.StampedLock;

import static net.openhft.chronicle.values.Values.newNativeReference;

public class DirtyReadVictim implements Runnable {
    private int isoLevel = ChronicleAcidIsolation.LOWEST_LATENCY;
    private ChronicleMap chm;
    private StampedLock offHeapLock;

    DirtyReadVictim(String isoL) {
        if (isoL.equals("DIRTY_READ_INTOLERANT"))
            this.isoLevel = ChronicleAcidIsolation.DIRTY_READ_INTOLERANT;
        else if (isoL.equals("DIRTY_READ_OPTIMISTIC"))
            this.isoLevel = ChronicleAcidIsolation.DIRTY_READ_OPTIMISTIC;
    }

    @Override
    public void run() {
        Scanner sc = new Scanner(System.in);
        try {

/**
 *  ben.cotton@rutgers.edu   START
 */
            Double coupon = 0.00;
            BondVOInterface bond = newNativeReference(BondVOInterface.class);
            long stamp = 0;
            System.out.println(
                    " ,,@t=" + System.currentTimeMillis() +
                            " DirtyReadVictim CALLING offHeapLock.tryOptimisticRead()"
            );
            while ((stamp = this.offHeapLock.tryOptimisticRead()) == 0) {
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
                                " DirtyReadVictim sleeping 60 seconds"
                );
                Thread.sleep(60_000);
            } finally {
                if (this.offHeapLock.validate(stamp)) {
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

    public ChronicleMap getCraig() {
        return this.chm;
    }

    public void setCraig(ChronicleMap craig) {
        this.chm = craig;
    }

    public void setStampedLock(StampedLock _sLock) {
        this.offHeapLock = _sLock;
    }
}

