package net.openhft.chronicle.map.fromdocs.acid;

public interface ChronicleAcidIsolation extends java.sql.Connection {

    int LOWEST_LATENCY = ChronicleAcidIsolation.TRANSACTION_NONE;
    int DIRTY_READ_OPTIMISTIC = ChronicleAcidIsolation.TRANSACTION_READ_UNCOMMITTED;
    int DIRTY_READ_INTOLERANT = ChronicleAcidIsolation.TRANSACTION_READ_COMMITTED;
    int REPEATABLE_READS_MANDATORY = ChronicleAcidIsolation.TRANSACTION_REPEATABLE_READ;
    int PHANTOM_READ_INTOLERANT = ChronicleAcidIsolation.TRANSACTION_SERIALIZABLE;

}

