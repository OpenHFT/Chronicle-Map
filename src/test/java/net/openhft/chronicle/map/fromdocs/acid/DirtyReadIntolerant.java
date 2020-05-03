package net.openhft.chronicle.map.fromdocs.acid;

import com.sun.org.apache.xpath.internal.operations.Bool;
import net.openhft.affinity.AffinitySupport;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.map.acid.ChronicleAcidIsolation;
import net.openhft.chronicle.map.acid.ChronicleAcidIsolationGovernor;
import net.openhft.chronicle.map.acid.BondVOInterface;
import net.openhft.chronicle.map.fromdocs.pingpong_latency.PingPongCASLeft;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;

import static net.openhft.chronicle.values.Values.newNativeReference;


public class DirtyReadIntolerant {

    public static void main(String... ignored) throws Exception {

        ChronicleMap<String, BondVOInterface> operand =
                DirtyReadIntolerant.acquireChronicleMapOperand(
                    "C:\\Users\\buddy\\dev\\shm\\OPERAND_CHRONICLE_MAP"
                );
        ChronicleMap<Thread,Integer> operator =
                DirtyReadIntolerant.acquireChronicleMapOperator(
                    "C:\\Users\\buddy\\dev\\shm\\OPERATOR_CHRONICLE_MAP"
                );
        ChronicleAcidIsolationGovernor chrAig = new ChronicleAcidIsolationGovernor();
        chrAig.setCompositeChronicleMap(operand);
        chrAig.setAutoCommit(Boolean.FALSE);
        chrAig.setTransactionIsolationMap(operator);
        System.out.println("Established chrAig");

        DirtyReadOffender offender = new DirtyReadOffender();
        offender.setCraig(chrAig);

        DirtyReadVictim victim = new DirtyReadVictim();
        victim.setCraig(chrAig);

        (new Thread(offender)).start();
        (new Thread(victim)).start();


    }

//

    static ChronicleMap<String, BondVOInterface> acquireChronicleMapOperand(String operand)
                                                                        throws IOException {
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

    static ChronicleMap<Thread, Integer> acquireChronicleMapOperator(String operator)
                                                                         throws IOException {
        // ensure thread ids are globally unique.
        AffinitySupport.setThreadId();
        return ChronicleMapBuilder.of(Thread.class, Integer.class)
                .entries(16)
                .averageKeySize("123456789".length())
                .createPersistedTo(
                        new File(
                                operator
                                //  "C:\\Users\\buddy\\dev\\shm\\OPERATOR_CHRONICLE_MAP"
                        )
                );
        //.create();
    }

//    static void offendPrice(ChronicleMap<String, BondVOInterface> chm, double _coupon,
//                             double _coupon2, boolean setFirst, final String desc) {
//        BondVOInterface bond1 = newNativeReference(BondVOInterface.class);
//        BondVOInterface bond2 = newNativeReference(BondVOInterface.class);
//        BondVOInterface bond3 = newNativeReference(BondVOInterface.class);
//        BondVOInterface bond4 = newNativeReference(BondVOInterface.class);
//
//        chm.acquireUsing("369604101", bond1);
//        chm.acquireUsing("369604102", bond2);
//        chm.acquireUsing("369604103", bond3);
//        chm.acquireUsing("369604104", bond4);
//        System.out.printf(
//                "\n\n" + desc +
//                ": Timing 1 x off-heap operations on /dev/shm/RDR_DIM_Mock\n"
//        );
//        if (setFirst) {
//            bond1.setCoupon(_coupon);
//            bond2.setCoupon(_coupon);
//            bond3.setCoupon(_coupon);
//            bond4.setCoupon(_coupon);
//        }
//        int timeToCallNanoTime = 30;
//        int runs = 1000000;
//        long[] timings = new long[runs];
//        for (int j = 0; j < 10; j++) {
//            for (int i = 0; i < runs; i++) {
//                long _start = System.nanoTime(); //
//                while (!bond1.compareAndSwapCoupon(_coupon, _coupon2)) ;
//                //while (!bond2.compareAndSwapCoupon(_coupon, _coupon2)) ;
//                //while (!bond3.compareAndSwapCoupon(_coupon, _coupon2)) ;
//                //while (!bond4.compareAndSwapCoupon(_coupon, _coupon2)) ;
//
//                timings[i] = (System.nanoTime() - _start - timeToCallNanoTime) / 4;
//            }
//            Arrays.sort(timings);
//            System.out.printf("#%d:  compareAndSwapCoupon() 50/90/99%%tile was %,d / %,d / %,d%n",
//                    j, timings[runs / 2], timings[runs * 9 / 10], timings[runs * 99 / 100]);
//        }
//    }
}
