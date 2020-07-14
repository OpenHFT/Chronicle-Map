package net.openhft.chronicle.map.fromdocs.acid.revelations;

import net.openhft.affinity.AffinitySupport;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.values.Values;

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
    ChronicleMap<String, LongValue> chmR;

    ChronicleStampedLockVOInterface offHeapLock =
            newNativeReference(ChronicleStampedLockVOInterface.class);
    ChronicleStampedLockVOInterface lastWriterT =
            newNativeReference(ChronicleStampedLockVOInterface.class);
    LongValue readLockHolderCount = Values.newNativeReference(LongValue.class);


    ChronicleStampedLock(String chronicelStampedLockLocality) {
        try {
            chm = offHeapLock(chronicelStampedLockLocality);
            chmR = offHeapLockReaderCount(chronicelStampedLockLocality+"=ReaderCount");
            chm.acquireUsing("Stamp ", offHeapLock);
            chm.acquireUsing("LastWriterTime ", lastWriterT);
            chmR.acquireUsing("ReaderCount ", readLockHolderCount);
            //readLockHolderCount.setValue(0L);
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
        //do {
            l = (offHeapLock = chm.get("Stamp ")).getEntryLockState();
//            System.out.println(
//                    " ,,@t=" + System.currentTimeMillis() +
//                            " ChronicleStampedLock waiting for unlockWrite()... " +
//                            ""
//            );
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
       // } while ( /*l < 0L*/); // do not loop! @vgrazi confirms that in JCA rebooted app.

        offHeapLock.setEntryLockState(0L);
        long t = System.currentTimeMillis();
        chm.put("Stamp ", offHeapLock);
        System.out.println(
                " ,@t=" + System.currentTimeMillis() +
                        " ChronicleStampedLock saw stamp=["+l+"]"+
                        " tryOptmisticRead() returning stamp=" +
                        t +
                        ","
        );

        return (t);
    }

    @Override
    public boolean validate(long stamp) {
        offHeapLock = chm.get("Stamp ");
        lastWriterT = chm.get("LastWriterTime ");
        boolean ret = false;
        if (lastWriterT.getEntryLockState() > stamp || offHeapLock.getEntryLockState() <0L) {
            ret = Boolean.FALSE;
        } else {
            ret = Boolean.TRUE;
        }
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
            System.out.println(
                    " ,@t=" + System.currentTimeMillis() +
                            " ChronicleStampedLock tryWriterLock() WAITING " +
                            " on offHeapLock.unlock(" +
                            offHeapLock.getEntryLockState() +
                            ") ,"
            );
            offHeapLock = chm.get("Stamp ");
            l = offHeapLock.getEntryLockState();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } while (l != 0L || readLockHolderCount.getValue() > 0);
        System.out.println(
                " ,@t=" + System.currentTimeMillis() +
                        " ChronicleStampedLock tryWriterLock() RESUMING " +
                        " ,"
        );
        long t = System.currentTimeMillis();
        try {
            Thread.sleep((long) (Math.random() * 1000)); //sleep up to 1 second
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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
        readLockHolderCount  = chmR.get("ReaderCount ");

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
        try {
            Thread.sleep((long) (Math.random() * 2000)); //sleep up to 1 second
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(
                " ,@t=" + System.currentTimeMillis() +
                        " ChronicleStampedLock tryReadLock() RESUMING " +
                        //" readerCount=[" + readerCount.getReaderCount() + "]" +
                        " readerCount=[" + readLockHolderCount.getVolatileValue() + "]" +
                        " BEFORE addAtomic(1) "+
                        ","
        );

        readLockHolderCount.addAtomicValue(1);


        try {
            Thread.sleep((long) (Math.random() * 1000)); //sleep up to 1 second
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        offHeapLock.setEntryLockState(
                readLockHolderCount.getVolatileValue()
        ); //negative ==> Writer holds StampedLock

        chm.put("Stamp ", offHeapLock);

        System.out.println(
                " ,@t=" + System.currentTimeMillis() +
                        " ChronicleStampedLock tryReadLock() returned stamp=" +
                        offHeapLock.getEntryLockState() +
                        " readerCount=[" + readLockHolderCount.getVolatileValue() + "]" +
                        " AFTER addAtomic(1) "+
                        ","
        );
        chmR.put("ReaderCount ", readLockHolderCount);
        return (offHeapLock.getEntryLockState());
    }

    @Override
    public void unlockRead(long stamp) {
        offHeapLock = chm.get("Stamp ");
        readLockHolderCount  = chmR.get("ReaderCount ");

        System.out.println(
                " ,@t=" + System.currentTimeMillis() +
                        " ChronicleStampedLock unlockRead(" + stamp + ") unlocking.." +
                        "ReaderCount=[" + readLockHolderCount.getVolatileValue() + "]" +
                        " BEFORE addAtomic(-1) "+
                        ","
        );
        readLockHolderCount.addAtomicValue(-1);
        System.out.println(
                " ,@t=" + System.currentTimeMillis() +
                        " ChronicleStampedLock unlockRead(" + stamp + ") unlocking.." +
                        "ReaderCount=[" + readLockHolderCount.getVolatileValue() + "]" +
                        " AFTER addAtomic(-1) "+
                        ","
        );
        offHeapLock.setEntryLockState(readLockHolderCount.getVolatileValue());

        System.out.println(
                " ,@t=" + System.currentTimeMillis() +
                        "offHeapLock=[" + offHeapLock.getEntryLockState() + "]" +
                        ","
        );
        chm.put("Stamp ", offHeapLock);
        chmR.put("ReaderCount ", readLockHolderCount);
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
                        chm.get("Stamp ").getEntryLockState() +
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

    static ChronicleMap<String, LongValue> offHeapLockReaderCount(String operand)
            throws IOException {

        AffinitySupport.setThreadId();
        return ChronicleMapBuilder.of(String.class, LongValue.class)
                .entries(16)
                .averageKeySize("123456789".length())
                .createPersistedTo(
                        new File(
                                operand
                        )
                );
    }
}
