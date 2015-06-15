package net.openhft.chronicle.hash.replication;

public interface ReplicableEntry {
    byte originIdentifier();

    long originTimestamp();
}
