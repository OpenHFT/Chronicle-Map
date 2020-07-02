package net.openhft.chronicle.map.fromdocs.acid.revelations;


import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.fromdocs.BondVOInterface;
import net.openhft.chronicle.map.fromdocs.acid.revelations.DirtyReadTolerance;

import java.util.Scanner;
import java.util.concurrent.locks.StampedLock;

import static net.openhft.chronicle.values.Values.newNativeReference;

public class DirtyReadOffender  {

    public static void main(String args[]) {
        Scanner sc = new Scanner(System.in);

        try {
            String isoLevel = args[0];

            ChronicleMap<String, BondVOInterface> chm =
                    DirtyReadTolerance.offHeap(
                            "C:\\Users\\buddy\\dev\\shm\\OPERAND_CHRONICLE_MAP"
                    );
            System.out.println(
                    " @t=" + System.currentTimeMillis() +
                            " DirtyReadOffender established chm "
            );
            BondVOInterface bond = newNativeReference(BondVOInterface.class);
            chm.acquireUsing("369604101", bond);
            System.out.println(
                    " @t=" + System.currentTimeMillis() +
                            " DirtyReadOffender sleeping 10 seconds "
            );
            Thread.sleep(10 * 1_000);
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
            ChronicleStampedLock offHeapLock = new ChronicleStampedLock();
            while ((stamp = offHeapLock.writeLock()) == 0) {
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
                                " DirtyReadOffender "+
                                " calling chm.put('369604101',"+newCoupon+") "
                );
                bond.setCoupon(newCoupon);
                chm.put("369604101", bond);
                System.out.println(
                        " @t=" + System.currentTimeMillis() +
                                " DirtyReadOffender coupon=["+
                                bond.getCoupon()+
                                "] written. "
                );
            } finally {
                offHeapLock.unlockWrite(stamp);
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
        } finally {
            System.out.println(
                    " ,,@t=" + System.currentTimeMillis() +
                            " DirtyReadOffender COMMITTED"
            );
        }
    }

}
