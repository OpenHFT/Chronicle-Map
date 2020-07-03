package net.openhft.chronicle.map.fromdocs.acid.revelations;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.fromdocs.BondVOInterface;

import java.io.IOException;
import java.util.concurrent.locks.StampedLock;

import static net.openhft.chronicle.values.Values.newNativeReference;

/**
 * ben.cotton@rutgers.edu
 * <p>
 * A totally hacked impl, awaiting OpenHFT's official ChronicleStampedLock API
 */
public class ChronicleStampedLock extends StampedLock {

    ChronicleMap<String, BondVOInterface> chm;
    BondVOInterface offHeapLock = newNativeReference(BondVOInterface.class);
    BondVOInterface lastWriterT = newNativeReference(BondVOInterface.class);
    {
        try {
            chm = DirtyReadTolerance.offHeap(
                    "C:\\Users\\buddy\\dev\\shm\\OPERAND_CHRONICLE_MAP"
            );
            chm.acquireUsing("Stamp ", offHeapLock); //mock'd
            chm.acquireUsing("LastWriterTime ", lastWriterT);
            System.out.println(
                    " ,@t=" + System.currentTimeMillis() +
                            " ChronicleStampedLock constructed" +
                            ","
            );
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public long tryOptimisticRead() {
        long l = 0;

        offHeapLock = chm.get("Stamp ");
        do {
            l = offHeapLock.getEntryLockState();
        } while (l < 0L); // wait until any holding Writer unlocks

        offHeapLock.setEntryLockState(0L);

        chm.put("Stamp ", offHeapLock);
        System.out.println(
                " ,@t=" + System.currentTimeMillis() +
                        " ChronicleStampedLock tryOptmisticRead() returned stamp=" +
                        l +
                        ","
        );
        long t = System.currentTimeMillis();
        return (t);
    }

    @Override
    public boolean validate(long stamp) {
        offHeapLock = chm.get("Stamp ");
        lastWriterT = chm.get("LastWriterTime ");
        boolean ret = false;
        if (lastWriterT.getEntryLockState() > stamp) {
            ret = Boolean.FALSE;
        } else
            ret = Boolean.TRUE;
        System.out.println(
                " ,@t=" + System.currentTimeMillis() +
                        " ChronicleStampedLock validate(" +
                        stamp + ") returned =[" +
                        ret + "] " +
                        ","
        );
        System.out.println(
                " ,@t=" + System.currentTimeMillis() +
                        " ChronicleStampedLock LastWriterT=[" +
                        lastWriterT.getEntryLockState() + "] " +
                        ","
        );
        return ret;
    }

    @Override
    public long tryWriteLock() {
        long l = 0;

        offHeapLock = chm.get("Stamp ");
        lastWriterT = chm.get("LastWriterTime ");
        do {
            l = offHeapLock.getEntryLockState();
        } while (l != 0L);

        long t = System.currentTimeMillis();
        offHeapLock.setEntryLockState(-t); //negative ==> Writer holds StampedLock
        lastWriterT.setEntryLockState(t);
        chm.put("Stamp ", offHeapLock);
        chm.put("LastWriterTime ", lastWriterT);
        System.out.println(
                " ,@t=" + t +
                        " ChronicleStampedLock tryWriteLock() returned stamp=" +
                        offHeapLock.getEntryLockState() +
                        ","
        );
        return (offHeapLock.getEntryLockState());
    }

    @Override
    public void unlockWrite(long stamp) {
        System.out.println(
                " ,@t=" + System.currentTimeMillis() +
                        " ChronicleStampedLock unlockWrite(" + stamp + ") unlocking..,"
        );
        offHeapLock = chm.get("Stamp ");
        offHeapLock.setEntryLockState(0L);
        chm.put("Stamp ", offHeapLock);
        System.out.println(
                " ,@t=" + System.currentTimeMillis() +
                        " ChronicleStampedLock unlockWrite(" +
                        stamp +
                        ") unlocked. set to Zero" +
                        ","
        );
    }

}
