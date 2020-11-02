package net.openhft.chronicle.map.fromdocs.acid.genesis;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.fromdocs.BondVOInterface;

import java.util.Scanner;
import java.util.concurrent.locks.StampedLock;

import static net.openhft.chronicle.values.Values.newNativeReference;

public class DirtyReadOffender implements Runnable {
    private int isoLevel;
    private ChronicleMap chm;
    private StampedLock offHeapLock;

    @Override
    public void run() {
        Scanner sc = new Scanner(System.in);

        try {
            BondVOInterface bond = newNativeReference(BondVOInterface.class);
            this.chm.acquireUsing("369604101", bond);
            System.out.println(
                    " @t=" + System.currentTimeMillis() +
                            " DirtyReadOffender sleeping 60 seconds "
            );
            Thread.sleep(60 * 1_000);
            System.out.println(
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
                    " @t=" + System.currentTimeMillis() +
                            " DirtyReadOffender ACQUIRING offHeapLock.writeLock();"
            );
            while ((stamp = this.offHeapLock.writeLock()) == 0) {
                ;
            }
            System.out.println(
                    " @t=" + System.currentTimeMillis() +
                            " DirtyReadOffender ACQUIRED offHeapLock.writeLock();"
            );
            try {
                double newCoupon = 3.5 + Math.random();
                System.out.println(
                        " @t=" + System.currentTimeMillis() +
                                " DirtyReadOffender " +
                                " calling chm.put('369604101'," + newCoupon + ") "
                );
                bond.setCoupon(newCoupon);
                chm.put("369604101", bond);
                System.out.println(
                        " @t=" + System.currentTimeMillis() +
                                " DirtyReadOffender coupon=[" +
                                bond.getCoupon() +
                                "] written. "
                );
            } finally {
                this.offHeapLock.unlockWrite(stamp);
                System.out.println(
                        " @t=" + System.currentTimeMillis() +
                                " DirtyReadOffender called offHeapLock.unlockWrite(stamp);"
                );
            }
            /**
             *  ben.cotton@rutgers.edu
             *
             *  END
             *
             */
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

    public int getIsoLevel() {
        return isoLevel;
    }

    public void setIsoLevel(int isoLevel) {
        this.isoLevel = isoLevel;
    }

    public void setStampedLock(StampedLock _sLock) {
        this.offHeapLock = _sLock;
    }
}
