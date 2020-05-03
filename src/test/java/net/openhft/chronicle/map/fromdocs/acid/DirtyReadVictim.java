package net.openhft.chronicle.map.fromdocs.acid;


import net.openhft.chronicle.map.acid.BondVOInterface;
import net.openhft.chronicle.map.acid.ChronicleAcidIsolation;
import net.openhft.chronicle.map.acid.ChronicleAcidIsolationGovernor;

import java.sql.SQLException;
import java.util.Scanner;

import static net.openhft.chronicle.values.Values.newNativeReference;

public class DirtyReadVictim implements Runnable {

    ChronicleAcidIsolationGovernor chrAig;

    public ChronicleAcidIsolationGovernor getCraig() {

        return this.chrAig;
    }

    public void setCraig(ChronicleAcidIsolationGovernor craig) {

        this.chrAig = craig;
    }

    @Override
    public void run() {
        Scanner sc= new Scanner(System.in);
        try {
            Thread.sleep(10_000);
            BondVOInterface bond = newNativeReference(BondVOInterface.class);
            chrAig.getCompositeChronicleMap().acquireUsing("369604101", bond);
            System.out.println(
                    " ---------- @t="+System.currentTimeMillis()+
                    " DirtyReadVictim calling m.get('369604101')"
            );
            //sc.nextLine();
            chrAig.setTransactionIsolation(ChronicleAcidIsolation.DIRTY_READ_INTOLERANT);
            bond = chrAig.getCompositeChronicleMap().get("369604101");
            Double coupon = bond.getCoupon();
            System.out.println(
                            " ---------- @t="+System.currentTimeMillis()+
                            " DirtyReadVictim calling chrAig.commmit()"
            );
            //sc.nextLine();
            chrAig.commit();
            System.out.println(
                    " ---------- @t="+System.currentTimeMillis()+
                    " DirtyReadVictim COMMITTED"
            );
        } catch (Exception throwables) {
            try {
                System.out.println(
                                " ---------- @t="+System.currentTimeMillis()+
                                " DirtyReadVictim calling chrAig.rollback()"
                );
                sc.nextLine();
                chrAig.rollback();
                System.out.println(
                        " ---------- @t="+System.currentTimeMillis()+
                        " DirtyReadVictim ROLLED BACK"
                );
            } catch (SQLException e) {
                e.printStackTrace();
            }
            throwables.printStackTrace();
        }


    }
}

