package net.openhft.chronicle.map.fromdocs.acid.revelations;

import net.openhft.affinity.AffinitySupport;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.StampedLock;

import static net.openhft.chronicle.values.Values.newNativeReference;

/**
 * ben.cotton@rutgers.edu
 * <p>
 * A totally hacked impl, awaiting OpenHFT's official ChronicleStampedLock API
 * <p>
 * A usable impl will more properly belong in the Chronicle-Algorithms repo
 */
public class ChronicleStampedLock extends StampedLock {

    ChronicleMap<String, ChronicleStampedLockVOInterface> chm;
    ChronicleStampedLockVOInterface offHeapLock =
            newNativeReference(ChronicleStampedLockVOInterface.class);
    ChronicleStampedLockVOInterface lastWriterT =
            newNativeReference(ChronicleStampedLockVOInterface.class);
    ChronicleStampedLockVOInterface readerCount =
            newNativeReference(ChronicleStampedLockVOInterface.class);

    ChronicleStampedLock(String chronicelStampedLockLocality) {
        try {
            chm = offHeapLock(chronicelStampedLockLocality);
            chm.acquireUsing("Stamp ", offHeapLock);
            chm.acquireUsing("LastWriterTime ", lastWriterT);
            chm.acquireUsing("ReaderCount ", readerCount);
            System.out.println(
                    " ,@t=" + System.currentTimeMillis() +
                            " ChronicleStampedLock constructed" +
                            ","
            );
            readerCount.setEntryLockState(0L);
            chm.put("ReadederCount ", readerCount);
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
        readerCount = chm.get("ReaderCount ");

        do {
            System.out.println(
                    " ,@t=" + System.currentTimeMillis() +
                            " ChronicleStampedLock tryWriterLock() WAITING " +
                            " on offHeapLock.unlock(" +
                            offHeapLock.getEntryLockState() +
                            ") ,"
            );
            offHeapLock = chm.get("Stamp ");
            l = offHeapLock.getEntryLockState();
            readerCount = chm.get("ReaderCount ");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } while (l != 0L || readerCount.getEntryLockState() > 0);
        System.out.println(
                " ,@t=" + System.currentTimeMillis() +
                        " ChronicleStampedLock tryWriterLock() RESUMING " +
                        " ,"
        );
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
    public long tryReadLock() {
        long l = 0;

        offHeapLock = chm.get("Stamp ");
        lastWriterT = chm.get("LastWriterTime ");
        readerCount = chm.get("ReaderCount ");

        do {
            System.out.println(
                    " ,@t=" + System.currentTimeMillis() +
                            " ChronicleStampedLock tryReadLock() WAITING on Writer.unlock(" +
                            offHeapLock.getEntryLockState() +
                            ") ,"
            );
            l = (offHeapLock = chm.get("Stamp ")).getEntryLockState();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } while (l < 0L);
        System.out.println(
                " ,@t=" + System.currentTimeMillis() +
                        " ChronicleStampedLock tryReadLock() RESUMING ,"
        );

        readerCount.addAtomicReaderCount( 1);

        long t = System.currentTimeMillis();
        offHeapLock.setEntryLockState(t); //negative ==> Writer holds StampedLock

        chm.put("Stamp ", offHeapLock);
        chm.put("ReaderCount ", readerCount);

        System.out.println(
                " ,@t=" + t +
                        " ChronicleStampedLock tryReadLock() returned stamp=" +
                        offHeapLock.getEntryLockState() +
                        " readerCount=["+readerCount.getReaderCount()+"]"+
                        ","
        );
        return (offHeapLock.getEntryLockState());
    }

    @Override
    public void unlockRead(long stamp) {
        offHeapLock = chm.get("Stamp ");
        readerCount = chm.get("ReaderCount ");
        System.out.println(
                " ,@t=" + System.currentTimeMillis() +
                        " ChronicleStampedLock unlockRead(" + stamp + ") unlocking.."+
                        "ReaderCount="+readerCount.getReaderCount()+
                ","
        );

        readerCount.addAtomicReaderCount(-1);
        if (readerCount.getReaderCount() == 0) {
            offHeapLock.setEntryLockState(0L);
        }
        System.out.println(
                " ,@t=" + System.currentTimeMillis() +
                        "ReaderCount="+readerCount.getReaderCount()+
                        ","
        );
        chm.put("Stamp ", offHeapLock);
        chm.put("ReaderCount ", readerCount);
        System.out.println(
                " ,@t=" + System.currentTimeMillis() +
                        " ChronicleStampedLock unlockRead(" +
                        stamp +
                        ") unlocked. set to Zero. ReaderCount=" +
                        readerCount.getReaderCount() +
                        ","
        );
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

    static ChronicleMap<String, ChronicleStampedLockVOInterface> offHeapLock(String operand)
            throws IOException {

        AffinitySupport.setThreadId();
        return ChronicleMapBuilder.of(String.class, ChronicleStampedLockVOInterface.class)
                .entries(16)
                .averageKeySize("123456789".length())
                .createPersistedTo(
                        new File(
                                operand
                        )
                );
    }
}
