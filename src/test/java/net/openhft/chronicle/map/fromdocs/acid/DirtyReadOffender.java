package net.openhft.chronicle.map.fromdocs.acid;

import net.openhft.chronicle.map.acid.ChronicleAcidIsolation;
import net.openhft.chronicle.map.acid.ChronicleAcidIsolationGovernor;
import net.openhft.chronicle.map.acid.BondVOInterface;

import java.sql.SQLException;

import static net.openhft.chronicle.values.Values.newNativeReference;

public class DirtyReadOffender implements Runnable {

    ChronicleAcidIsolationGovernor chraig;

    public ChronicleAcidIsolationGovernor getCraig() {
        return chraig;
    }

    public void setCraig(ChronicleAcidIsolationGovernor craig) {
        this.chraig = craig;
    }


    @Override
    public void run() {

        try {
            BondVOInterface bond = newNativeReference(BondVOInterface.class);
            chraig.getCompositeChronicleMap().acquireUsing("369604101", bond);
            chraig.setTransactionIsolation(ChronicleAcidIsolation.LOWEST_LATENCY);
            bond.setCoupon(4.55);
            chraig.put("369604101",bond);
            chraig.commit();
        } catch (Exception throwables) {
            try {
                chraig.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            throwables.printStackTrace();
        }

    }

}
