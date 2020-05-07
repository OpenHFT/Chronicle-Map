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


public class DirtyReadTolerance<K,V> {

    public static void main(String args[]) throws Exception {

        System.out.println("DirtyReadOffender,chrAig["+args[0]+"]coupon=3.50,DirtyReadVictim");
        String isoLevel = args[0];

        ChronicleMap<String, BondVOInterface> operand =
                DirtyReadTolerance.acquireChronicleMapOperand(
                        "C:\\Users\\buddy\\dev\\shm\\OPERAND_CHRONICLE_MAP"
                );
        ChronicleMap<String, Integer> operator =
                DirtyReadTolerance.acquireChronicleMapOperator(
                        "C:\\Users\\buddy\\dev\\shm\\OPERATOR_CHRONICLE_MAP"
                );
        ChronicleAcidIsolationGovernor chrAig = new ChronicleAcidIsolationGovernor();
        chrAig.setCompositeChronicleMap(operand);
        chrAig.setAutoCommit(Boolean.FALSE);
        chrAig.setTransactionIsolationMap(operator);
        System.out.println(",Established chrAig,");

        DirtyReadOffender offender = new DirtyReadOffender();
        offender.setCraig(chrAig);

        DirtyReadVictim victim = new DirtyReadVictim(isoLevel);
        victim.setCraig(chrAig);

        (new Thread(offender)).start();
        (new Thread(victim)).start();


    }

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

    static ChronicleMap<String, Integer> acquireChronicleMapOperator(String operator)
            throws IOException {
        // ensure thread ids are globally unique.
        AffinitySupport.setThreadId();
        return ChronicleMapBuilder.of(String.class, Integer.class)
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
}