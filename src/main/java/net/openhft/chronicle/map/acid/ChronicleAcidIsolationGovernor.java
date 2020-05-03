package net.openhft.chronicle.map.acid;

//import net.openhft.affinity.AffinitySupport;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.map.acid.BondVOInterface;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class ChronicleAcidIsolationGovernor implements ChronicleAcidIsolation {

    private ChronicleMap<Thread, Integer> transactionIsolationMap;
    private ChronicleMap<String, BondVOInterface> compositeChronicleMap; //hacked in, not generic
    private String priorCusip;
    private Double  priorCoupon;


    public synchronized void put(String cusip, BondVOInterface bond) throws Exception {

        ChronicleMap<String,BondVOInterface> cMap = this.getCompositeChronicleMap();
        this.priorCusip = cusip;
        this.priorCoupon = cMap.get(cusip).getCoupon();

        bond.setCoupon(priorCoupon + 0.001);
        this.getCompositeChronicleMap().put(cusip, bond);
    }


    //here is where the drama happens
    public synchronized BondVOInterface get(String cusip) throws Exception {
        System.out.println(
                        " ---------- @t="+System.currentTimeMillis()+
                        " Tx="+Thread.currentThread()+
                        " m.get()"+
                        "---------- "
        );
        BondVOInterface b = null;
        Thread tx = Thread.currentThread();
        ChronicleMap<Thread,Integer> txMap = this.getTransactionIsolationMap();
        if (txMap.size() > 1) { //other ACID transactions are active
            if (txMap.get(tx) <= ChronicleAcidIsolation.DIRTY_READ_OPTIMISTIC) {
                b = this.compositeChronicleMap.get(cusip);
            } else if (txMap.get(tx) >= ChronicleAcidIsolation.DIRTY_READ_INTOLERANT) {
                System.out.println(
                                " ---------- @t="+System.currentTimeMillis()+
                                " Tx="+Thread.currentThread()+
                                " m.get() WAITING"+
                                "---------- "
                );
                this.wait();
                System.out.println(
                                " ---------- @t="+System.currentTimeMillis()+
                                " Tx="+Thread.currentThread()+
                                " m.get() RESUMING"+
                                "---------- "
                );
                b = this.compositeChronicleMap.get(cusip);
            }
        } else {
            b = this.compositeChronicleMap.get(cusip);
        }
        System.out.println(
                        " ---------- @t="+System.currentTimeMillis()+
                        " Tx="+Thread.currentThread()+
                        " m.get() DONE"+
                        "---------- "
        );
        return b;
    }

    public ChronicleMap<Thread, Integer> getTransactionIsolationMap() {

        return transactionIsolationMap;
    }

    public void setTransactionIsolationMap(ChronicleMap<Thread, Integer> transactionIsolationMap) {
        this.transactionIsolationMap = transactionIsolationMap;
    }

    public ChronicleMap<String, BondVOInterface> getCompositeChronicleMap() {

        return compositeChronicleMap;
    }

    public void setCompositeChronicleMap(ChronicleMap<String, BondVOInterface> ccm) {

        this.compositeChronicleMap = ccm;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {

        this.getTransactionIsolationMap().put(
                Thread.currentThread(),
                level
        );
    }

    @Override
    public int getTransactionIsolation() throws SQLException {

        int iL = this.getTransactionIsolationMap().get(Thread.currentThread());
        return(iL);
    }

    @Override
    public synchronized void commit() throws SQLException {
        System.out.println(
                " ---------- @t="+System.currentTimeMillis()+
                        " Tx="+Thread.currentThread()+
                        " chrAig.commit() BEGIN "+
                        "---------- "
        );
        this.getTransactionIsolationMap().remove(Thread.currentThread());
        System.out.println(
                " ---------- @t="+System.currentTimeMillis()+
                        " Tx="+Thread.currentThread()+
                        " chrAig.commit() END "+
                        "---------- "
        );
        this.notifyAll();
        System.out.println(
                " ---------- @t="+System.currentTimeMillis()+
                        " Tx="+Thread.currentThread()+
                        " chrAig.commit() complete notifyAll() to waiting Tx Threads "+
                        "---------- "
        );

    }

    @Override
    public synchronized void rollback() throws SQLException {
        BondVOInterface priorBond = this.getCompositeChronicleMap().get("369604101");
        if (priorBond != null) {
            priorBond.setCoupon(priorCoupon);
            this.getCompositeChronicleMap().put("369604101", priorBond);
        }
        System.out.println(
                " ---------- @t="+System.currentTimeMillis()+
                        " Tx="+Thread.currentThread()+
                        " chrAig.rollback() BEGIN "+
                        "---------- "
        );
        this.getTransactionIsolationMap().remove(Thread.currentThread());
        System.out.println(
                " ---------- @t="+System.currentTimeMillis()+
                        " Tx="+Thread.currentThread()+
                        " chrAig.rollback() COMPLETE "+
                        "---------- "
        );
        this.notifyAll();
        System.out.println(
                " ---------- @t="+System.currentTimeMillis()+
                        " Tx="+Thread.currentThread()+
                        " chrAig.rollback() completed notifyAll() to waiting Tx Threads"+
                        "---------- "
        );
    }
    // rest of these java.sql.Connection methods remain unimplemented ...
    // pedantics are too
    // intense.  In the real world, even in Captital Markets 'dirty read' intolerance
    // is likely the only isolation level Chronicle would accommodate.


    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {

    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return false;
    }


    @Override
    public void rollback(Savepoint savepoint) throws SQLException {

    }

    @Override
    public void close() throws SQLException {

    }



    @Override
    public Statement createStatement() throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return null;
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return null;
    }


    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {

    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {

    }

    @Override
    public String getCatalog() throws SQLException {
        return null;
    }


    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return null;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return null;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

    }

    @Override
    public void setHoldability(int holdability) throws SQLException {

    }

    @Override
    public int getHoldability() throws SQLException {
        return 0;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return null;
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return null;
    }


    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {

    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return null;
    }

    @Override
    public Clob createClob() throws SQLException {
        return null;
    }

    @Override
    public Blob createBlob() throws SQLException {
        return null;
    }

    @Override
    public NClob createNClob() throws SQLException {
        return null;
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return null;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return false;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {

    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {

    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return null;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return null;
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return null;
    }

    @Override
    public void setSchema(String schema) throws SQLException {

    }

    @Override
    public String getSchema() throws SQLException {
        return null;
    }

    @Override
    public void abort(Executor executor) throws SQLException {

    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {

    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return 0;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }


}
