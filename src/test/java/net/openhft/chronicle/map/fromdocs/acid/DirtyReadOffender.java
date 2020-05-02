package net.openhft.chronicle.map.fromdocs.acid;

import net.openhft.chronicle.map.acid.ChronicleAcidIsolation;
import net.openhft.chronicle.map.acid.ChronicleAcidIsolationGovernor;
import net.openhft.chronicle.map.acid.BondVOInterface;

import java.sql.SQLException;

import static net.openhft.chronicle.values.Values.newNativeReference;

public class DirtyReadOffender implements Runnable {

    ChronicleAcidIsolationGovernor craig;

    public ChronicleAcidIsolationGovernor getCraig() {
        return craig;
    }

    public void setCraig(ChronicleAcidIsolationGovernor craig) {
        this.craig = craig;
    }


    @Override
    public void run() {

        try {
            BondVOInterface bond = newNativeReference(BondVOInterface.class);
            craig.getCompositeChronicleMap().acquireUsing("369604101", bond);
            craig.setTransactionIsolation(ChronicleAcidIsolation.LOWEST_LATENCY);
            bond.setCoupon(4.55);
            craig.put("369604101",bond);
            craig.commit();
        } catch (Exception throwables) {
            try {
                craig.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            throwables.printStackTrace();
        }

    }

}
