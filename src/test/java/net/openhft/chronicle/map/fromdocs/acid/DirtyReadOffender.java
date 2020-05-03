package net.openhft.chronicle.map.fromdocs.acid;

import net.openhft.chronicle.map.acid.ChronicleAcidIsolation;
import net.openhft.chronicle.map.acid.ChronicleAcidIsolationGovernor;
import net.openhft.chronicle.map.acid.BondVOInterface;

import java.sql.SQLException;
import java.util.Scanner;

import static net.openhft.chronicle.values.Values.newNativeReference;

public class DirtyReadOffender implements Runnable {

    ChronicleAcidIsolationGovernor chrAig;

    public ChronicleAcidIsolationGovernor getCraig() {
        return chrAig;
    }

    public void setCraig(ChronicleAcidIsolationGovernor craig) {
        this.chrAig = craig;
    }


    @Override
    public void run() {
        Scanner sc = new Scanner(System.in);

        try {
            BondVOInterface bond = newNativeReference(BondVOInterface.class);
            chrAig.getCompositeChronicleMap().acquireUsing("369604101", bond);
            System.out.println(
                            " @t=" + System.currentTimeMillis() +
                            " DirtyReadOffender calling chrAig.put('369604101',4.55) ---------- "
            );
            //sc.nextLine();
            chrAig.setTransactionIsolation(ChronicleAcidIsolation.LOWEST_LATENCY);
            bond.setCoupon(4.55);
            chrAig.put("369604101", bond);
            System.out.println(
                   " @t=" + System.currentTimeMillis() +
                   " DirtyReadOffender calling chrAig.commit()  ---------- "
            );
            sc.nextLine();
            chrAig.commit();
            System.out.println(
                    " @t=" + System.currentTimeMillis() +
                    " DirtyReadOffender COMMITTED ---------- "
            );
        } catch (Exception throwables) {
            try {
                System.out.println(
                        " @t=" + System.currentTimeMillis() +
                        " DirtyReadOffender calling chrAig.rollback ---------- "
                );
                chrAig.rollback();
                System.out.println(
                     " @t=" + System.currentTimeMillis() +
                     " DirtyReadOffender ROLLED BACK---------- "
                );
            } catch (SQLException e) {
                e.printStackTrace();
            }
            throwables.printStackTrace();
        }

    }

}
