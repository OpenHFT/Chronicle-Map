package net.openhft.chronicle.map.fromdocs.acid.revelations;

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
 * A usable 'reference' impl will more properly belong in the Chronicle-Algorithms/ repo
 */
public class ChronicleStampedLock extends StampedLock {

    ChronicleMap<String, ChronicleStampedLockVOInterface> chm;  //custody of StampedLock semantics
    ChronicleMap<String, LongValue> chmR;                       //Chronicle AtomicLong re: Reader set custody
    ChronicleStampedLockVOInterface offHeapLock =
            newNativeReference(ChronicleStampedLockVOInterface.class);  //the off-heap ChronicleStampedLock
    ChronicleStampedLockVOInterface lastWriterT =
            newNativeReference(ChronicleStampedLockVOInterface.class);   //needed to facilitate validate(stamp)
    LongValue readLockHolderCount = Values.newNativeReference(LongValue.class); //ReaderSet cardinality


    ChronicleStampedLock(String chronicelStampedLockLocality) { // path of Operand set i.e. /dev/shm/
        try {
            chm = offHeapLock(chronicelStampedLockLocality);
            chmR = offHeapLockReaderCount(chronicelStampedLockLocality+"=ReaderCount");
            chm.acquireUsing("Stamp ", offHeapLock); // K="Stamp ", V=OffHeapLock impl
            chm.acquireUsing("LastWriterTime ", lastWriterT); //needed for validate
            chmR.acquireUsing("ReaderCount ", readLockHolderCount);
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
        (offHeapLock = chm.get("Stamp ")).setEntryLockState(0L);
        long t = System.currentTimeMillis();
        chm.put("Stamp ", offHeapLock);
        System.out.println(
                " ,@t=" + System.currentTimeMillis() +
                        " ChronicleStampedLock saw stamp=["+0L+"]"+
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
        /**
         *  If *any* Writer interacted with the offHeapLock, since event=tryOptimisticRead(),
         *  then FAIL the validate() invoke.
         *
         *  Upon FAILURE, the thread on tryOptimisticRead() must apply its PESSIMISTIC policy (Thread
         *  has endured a DIRTY_READ.)
         *
         */
        if (lastWriterT.getEntryLockState() > stamp || offHeapLock.getEntryLockState() < 0L) {
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
        long l = 0L;

        offHeapLock = chm.get("Stamp ");
        lastWriterT = chm.get("LastWriterTime ");

        do {
            System.out.println(
                    " ,@t=" + System.currentTimeMillis() +
                            " ChronicleStampedLock tryWriterLock() ?WAITING " +
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
        } while (l != 0L || readLockHolderCount.getVolatileValue() > 0);
        System.out.println(
                " ,@t=" + System.currentTimeMillis() +
                        " ChronicleStampedLock tryWriterLock() PROCEEDING " +
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
        long l = 0L;

        offHeapLock = chm.get("Stamp ");
        readLockHolderCount  = chmR.get("ReaderCount ");

        do {
            System.out.println(
                    " ,@t=" + System.currentTimeMillis() +
                            " ChronicleStampedLock tryReadLock() ?WAITING on Writer.unlock(" +
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
                        " ChronicleStampedLock tryReadLock() PROCEEDING " +
                        //" readerCount=[" + readerCount.getReaderCount() + "]" +
                        " readerCount=[" +
                        (readLockHolderCount = chmR.get("ReaderCount ")).getVolatileValue() + "]" +
                        " BEFORE addAtomic(1) "+
                        ","
        );

        readLockHolderCount.addAtomicValue(1); // INCREMENT  the cardinality of the Reader set
        chmR.put("ReaderCount ", readLockHolderCount); // and make it IPC visible

        offHeapLock.setEntryLockState(
                readLockHolderCount.getVolatileValue()
        ); // assign the Lock to most-recent Reader

        chm.put("Stamp ", offHeapLock); // make it IPC visible

        System.out.println(
                " ,@t=" + System.currentTimeMillis() +
                        " ChronicleStampedLock tryReadLock() returned stamp=" +
                        offHeapLock.getEntryLockState() +
                        " readerCount=[" + readLockHolderCount.getVolatileValue() + "]" +
                        " AFTER addAtomic(1) "+
                        ","
        );
        return (offHeapLock.getEntryLockState());
    }

    @Override
    public void unlockRead(long stamp) {
        offHeapLock = chm.get("Stamp ");
        readLockHolderCount  = chmR.get("ReaderCount ");

        System.out.println(
                " ,@t=" + System.currentTimeMillis() +
                        " ChronicleStampedLock unlockRead(" + stamp + ") unlocking.." +
                        "ReaderCount=[" +
                        (readLockHolderCount  = chmR.get("ReaderCount ")).getVolatileValue() + "]" +
                        " BEFORE addAtomic(-1) "+
                        ","
        );
        readLockHolderCount.addAtomicValue(-1); // DECREMENT  the cardinality of the Reader set
        chmR.put("ReaderCount ", readLockHolderCount); // make it IPC visible
        System.out.println(
                " ,@t=" + System.currentTimeMillis() +
                        " ChronicleStampedLock unlockRead(" + stamp + ") unlocking.." +
                        "ReaderCount=[" +
                        (readLockHolderCount  = chmR.get("ReaderCount ")).getVolatileValue() + "]" +
                        " AFTER addAtomic(-1) "+
                        ","
        );
        offHeapLock.setEntryLockState(
                (readLockHolderCount  = chmR.get("ReaderCount ")).getVolatileValue()
        );
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
                        (offHeapLock = chm.get("Stamp ")).getEntryLockState() +
                        ") unlocked. set to Zero" +
                        ","
        );
    }

    static ChronicleMap<String, ChronicleStampedLockVOInterface> offHeapLock(String operand)
            throws IOException {

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
