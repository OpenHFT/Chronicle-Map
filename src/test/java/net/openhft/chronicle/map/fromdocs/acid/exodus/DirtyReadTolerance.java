package net.openhft.chronicle.map.fromdocs.acid.exodus;

import net.openhft.affinity.AffinitySupport;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.map.fromdocs.BondVOInterface;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.StampedLock;

public class DirtyReadTolerance<K, V> {

    public static void main(String args[]) throws Exception {
        System.out.println("DirtyReadOffender,chrAig[" + args[0] + "]coupon=3.50,DirtyReadVictim");
        String isoLevel = args[0];

        ChronicleMap<String, BondVOInterface> chm =
                DirtyReadTolerance.offHeap(
                        "C:\\Users\\buddy\\dev\\shm\\OPERAND_CHRONICLE_MAP"
                );
        System.out.println(",Established chm,");

        StampedLock sLock = new StampedLock();
        DirtyReadOffender offender = new DirtyReadOffender();
        offender.setCraig(chm);
        offender.setStampedLock(sLock);

        DirtyReadVictim victim = new DirtyReadVictim(isoLevel);
        victim.setCraig(chm);
        victim.setStampedLock(sLock);

        (new Thread(offender)).start();
        (new Thread(victim)).start();
    }

    static ChronicleMap<String, BondVOInterface> offHeap(String operand) throws IOException {
        // ensure thread ids are globally unique.
        AffinitySupport.setThreadId();
        return ChronicleMapBuilder.of(String.class, BondVOInterface.class)
                .entries(16)
                .averageKeySize("123456789".length())
                .createPersistedTo(
                        new File(
                                operand
                                //  "C:\\Users\\buddy\\dev\\shm\\OPERAND_CHRONICLE_MAP"
                        )
                );
        //.create();
    }
}