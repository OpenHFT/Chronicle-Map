package net.openhft.chronicle.map.fromdocs.acid.revelations;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.fromdocs.BondVOInterface;
import java.util.Scanner;
import java.util.concurrent.locks.StampedLock;

import static net.openhft.chronicle.values.Values.newNativeReference;

public class DirtyReadOffender  {

    public static void main(String args[]) {
        Scanner sc = new Scanner(System.in);

        try {
            String isoLevel = args[0];
            long sleepT = Long.parseLong(args[1]);

            ChronicleMap<String, BondVOInterface> chm =
                    DirtyReadTolerance.offHeap(
                            "C:\\Users\\buddy\\dev\\shm\\OPERAND_CHRONICLE_MAP"
                    );
            System.out.println(
                    " @t=" + System.currentTimeMillis() +
                            " DirtyReadOffender established chm "
            );
            BondVOInterface bond = newNativeReference(BondVOInterface.class);
            BondVOInterface cslMock = newNativeReference(BondVOInterface.class);
            chm.acquireUsing("369604101", bond);
            chm.acquireUsing("Offender ", cslMock); // mock ChronicleStampLock
            System.out.println(
                    " @t=" + System.currentTimeMillis() +
                            " DirtyReadOffender sleeping "+sleepT+" seconds "
            );
            Thread.sleep(sleepT * 1_000);
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
            StampedLock offHeapLock = new ChronicleStampedLock();
            while ((stamp = offHeapLock.tryWriteLock()) == 0) {
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
                cslMock.setEntryLockState(System.currentTimeMillis()); //mock'd
                chm.put("Offender ",cslMock); //mock'd
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
                                " DirtyReadOffender called " +
                                "offHeapLock.unlockWrite(stamp);"
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
