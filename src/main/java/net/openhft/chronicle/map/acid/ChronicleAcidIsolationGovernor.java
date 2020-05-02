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
    private int transactionIsolation;
    private ChronicleMap<Thread, Integer> transactionIsolationMap; //to demo in same JVM at first
    private ChronicleMap<String, BondVOInterface> compositeChronicleMap; //hacked in
    private String priorCusip;
    private Double  priorCoupon;


    public synchronized void put(String cusip, BondVOInterface bond) throws Exception {
        this.priorCusip = cusip;
        this.priorCoupon = this.getCompositeChronicleMap().get(cusip).getCoupon();
        this.getTransactionIsolationMap().put(Thread.currentThread(), this.getTransactionIsolation());
        bond.setCoupon(priorCoupon + 0.5);
        this.getCompositeChronicleMap().put(cusip, bond);
    }


    //here is where the drama happens
    public synchronized BondVOInterface get(String cusip) throws InterruptedException {

        BondVOInterface b = null;
        if (this.getTransactionIsolationMap().size() > 1) { //other ACID transactions are active
            if (this.getTransactionIsolationMap().get(Thread.currentThread()) <= ChronicleAcidIsolation.DIRTY_READ_OPTIMISTIC) {
                b = this.compositeChronicleMap.get(cusip);
            } else if (this.getTransactionIsolationMap().get(Thread.currentThread()) >= ChronicleAcidIsolation.DIRTY_READ_INTOLERANT) {
                this.wait();
                b = this.compositeChronicleMap.get(cusip);
            }
        } else {
            b = this.compositeChronicleMap.get(cusip);
        }

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

    public void setCompositeChronicleMap(ChronicleMap<String, BondVOInterface> compositeChronicleMap) {
        this.compositeChronicleMap = compositeChronicleMap;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        this.transactionIsolation = level;
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return this.transactionIsolation;
    }
    @Override
    public synchronized void commit() throws SQLException {
        this.getTransactionIsolationMap().remove(Thread.currentThread());
        this.notifyAll();

    }

    @Override
    public synchronized void rollback() throws SQLException {
        BondVOInterface priorBond = this.compositeChronicleMap.get(priorCusip);
        priorBond.setCoupon(priorCoupon);
        this.getCompositeChronicleMap().put(priorCusip,priorBond);
        this.getTransactionIsolationMap().remove(Thread.currentThread());
        this.notifyAll();
    }


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


    // rest of these java.sql.Connection methods remain unimplemented ...
    // pedantics are too
    // intense.  In the real world, even in Captital Markets 'dirty read' intolerance
    // is likely the only isolation level Chronicle would accommodate.

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
