/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.fromdocs.acid;

//import net.openhft.affinity.AffinitySupport;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.fromdocs.BondVOInterface;

import java.sql.*;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class ChronicleAcidIsolationGovernor implements ChronicleAcidIsolation {

    private ChronicleMap<String, Integer> transactionIsolationMap;
    private ChronicleMap<String, BondVOInterface> compositeChronicleMap; //hacked in, not generic
    private String priorCusip;
    private Double aCoupon;

    public synchronized void put(String cusip, BondVOInterface bond) {

        System.out.println(
                " , @t=" + System.currentTimeMillis() +
                        " Tx=" + Thread.currentThread().toString().replaceAll(",", ".") +
                        " inside chrAig.put('" + cusip + "'/" + aCoupon + ") BEGIN" +
                        ", "
        );
        ChronicleMap<String, BondVOInterface> cMap = this.getCompositeChronicleMap();
        this.aCoupon = cMap.get(cusip).getCoupon();

        bond.setCoupon(aCoupon);
        cMap.put(cusip, bond);
        System.out.println(
                " , @t=" + System.currentTimeMillis() +
                        " Tx=" + Thread.currentThread().toString().replaceAll(",", ".") +
                        " inside chrAig.put('" + cusip + "'/" + aCoupon + ") DONE" +
                        ", "
        );
    }

    //here is where the drama happens
    public synchronized BondVOInterface get(String cusip) throws Exception {
        System.out.println(
                " , @t=" + System.currentTimeMillis() +
                        " Tx=" + Thread.currentThread().toString().replaceAll(",", ".") +
                        " inside chrAig.get('" + cusip + "') BEGIN" +
                        ", "
        );
        BondVOInterface b = null;
        String tx = Thread.currentThread().toString();
        ChronicleMap<String, Integer> txMap = this.getTransactionIsolationMap();
        if (txMap.size() > 1) { //other ACID transactions are active
            if (txMap.get(tx) <= ChronicleAcidIsolation.DIRTY_READ_OPTIMISTIC) {
                b = this.compositeChronicleMap.get(cusip);
            } else if (txMap.get(tx) >= ChronicleAcidIsolation.DIRTY_READ_INTOLERANT) {
                System.out.println(
                        ", @t=" + System.currentTimeMillis() +
                                " Tx=" + Thread.currentThread().toString().replaceAll(",", ".") +
                                " inside chrAig.get() WAITING" +
                                " ,"
                );
                this.wait();
                System.out.println(
                        " , @t=" + System.currentTimeMillis() +
                                " Tx=" + Thread.currentThread().toString().replaceAll(",", ".") +
                                " inside chrAig.get() RESUMING" +
                                ", "
                );
                b = this.compositeChronicleMap.get(cusip);
            }
        } else {
            b = this.compositeChronicleMap.get(cusip);
        }
        System.out.println(
                " , @t=" + System.currentTimeMillis() +
                        " Tx=" + Thread.currentThread().toString().replaceAll(",", ".") +
                        " inside chrAig.get() DONE" +
                        " ,"
        );
        return b;
    }

    public ChronicleMap<String, Integer> getTransactionIsolationMap() {

        return transactionIsolationMap;
    }

    public void setTransactionIsolationMap(ChronicleMap<String, Integer> txMap) {
        this.transactionIsolationMap = txMap;
    }

    public ChronicleMap<String, BondVOInterface> getCompositeChronicleMap() {

        return compositeChronicleMap;
    }

    public void setCompositeChronicleMap(ChronicleMap<String, BondVOInterface> ccm) {

        this.compositeChronicleMap = ccm;
    }

    @Override
    public synchronized int getTransactionIsolation() {

        return this.getTransactionIsolationMap().get(Thread.currentThread().toString());
    }

    @Override
    public synchronized void setTransactionIsolation(int level) {

        this.getTransactionIsolationMap().put(
                Thread.currentThread().toString(),
                level
        );
    }

    @Override
    public synchronized void commit() {
        System.out.println(
                " ,@t=" + System.currentTimeMillis() +
                        " Tx=" + Thread.currentThread().toString().replaceAll(",", ".") +
                        " chrAig.commit() BEGIN " +
                        ", "
        );
        this.getTransactionIsolationMap().remove(Thread.currentThread().toString());
        System.out.println(
                " , @t=" + System.currentTimeMillis() +
                        " Tx=" + Thread.currentThread().toString().replaceAll(",", ".") +
                        " chrAig.commit() END " +
                        ", "
        );
        this.notifyAll();
        System.out.println(
                ", @t=" + System.currentTimeMillis() +
                        " Tx=" + Thread.currentThread().toString().replaceAll(",", ".") +
                        " chrAig.commit() complete notifyAll() to waiting Tx Threads " +
                        ","
        );

    }

    @Override
    public synchronized void rollback() {
        BondVOInterface priorBond = this.getCompositeChronicleMap().get("369604101");
        if (priorBond != null) {
            priorBond.setCoupon(3.50);
            this.getCompositeChronicleMap().put("369604101", priorBond);
        }
        System.out.println(
                " , @t=" + System.currentTimeMillis() +
                        " Tx=" + Thread.currentThread().toString().replaceAll(",", ".") +
                        " chrAig.rollback() BEGIN " +
                        ", "
        );
        this.getTransactionIsolationMap().remove(Thread.currentThread().toString());
        System.out.println(
                " ,@t=" + System.currentTimeMillis() +
                        " Tx=" + Thread.currentThread().toString().replaceAll(",", ".") +
                        " chrAig.rollback() COMPLETE coupon=3.50" +
                        ", "
        );
        this.notifyAll();
        System.out.println(
                ", @t=" + System.currentTimeMillis() +
                        " Tx=" + Thread.currentThread().toString().replaceAll(",", ".") +
                        " chrAig.rollback() completed notifyAll() to waiting Tx Threads" +
                        ", "
        );
    }
    // rest of these java.sql.Connection methods remain unimplemented ...
    // pedantics are too
    // intense.  In the real world, even in Captital Markets 'dirty read' intolerance
    // is likely the only isolation level Chronicle would accommodate.

    @Override
    public boolean getAutoCommit() {
        return false;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {

    }

    @Override
    public void rollback(Savepoint savepoint) {

    }

    @Override
    public void close() {

    }

    @Override
    public Statement createStatement() {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql) {
        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql) {
        return null;
    }

    @Override
    public String nativeSQL(String sql) {
        return null;
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public DatabaseMetaData getMetaData() {
        return null;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public void setReadOnly(boolean readOnly) {

    }

    @Override
    public String getCatalog() {
        return null;
    }

    @Override
    public void setCatalog(String catalog) {

    }

    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {

    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) {
        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) {
        return null;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() {
        return Collections.emptyMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) {

    }

    @Override
    public int getHoldability() {
        return 0;
    }

    @Override
    public void setHoldability(int holdability) {

    }

    @Override
    public Savepoint setSavepoint() {
        return null;
    }

    @Override
    public Savepoint setSavepoint(String name) {
        return null;
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) {

    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) {
        return null;
    }

    @Override
    public Clob createClob() {
        return null;
    }

    @Override
    public Blob createBlob() {
        return null;
    }

    @Override
    public NClob createNClob() {
        return null;
    }

    @Override
    public SQLXML createSQLXML() {
        return null;
    }

    @Override
    public boolean isValid(int timeout) {
        return false;
    }

    @Override
    public void setClientInfo(String name, String value) {

    }

    @Override
    public String getClientInfo(String name) {
        return "";
    }

    @Override
    public Properties getClientInfo() {
        return new Properties();
    }

    @Override
    public void setClientInfo(Properties properties) {

    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) {
        return null;
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) {
        return null;
    }

    @Override
    public String getSchema() {
        return null;
    }

    @Override
    public void setSchema(String schema) {

    }

    @Override
    public void abort(Executor executor) {

    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) {

    }

    @Override
    public int getNetworkTimeout() {
        return 0;
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }
}
