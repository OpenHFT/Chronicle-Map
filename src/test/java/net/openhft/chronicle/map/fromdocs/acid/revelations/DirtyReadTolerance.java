package net.openhft.chronicle.map.fromdocs.acid.revelations;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.map.fromdocs.BondVOInterface;
import net.openhft.chronicle.map.fromdocs.acid.exodus.DirtyReadOffender;
import net.openhft.chronicle.map.fromdocs.acid.exodus.DirtyReadVictim;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.StampedLock;

public class DirtyReadTolerance<K, V> {

    public static void main(String args[]) throws Exception {
        System.out.println("DirtyReadOffender,chrAig[" + args[0] + "]coupon=3.50,DirtyReadVictim");
        String isoLevel = args[0];

        ChronicleMap<String, BondVOInterface> chm =
                DirtyReadTolerance.offHeap(
                        args[2]
                                + "OPERAND_CHRONICLE_MAP"
                );
        System.out.println(",Established chm,");
    }

    static ChronicleMap<String, BondVOInterface> offHeap(String operand) throws IOException {

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