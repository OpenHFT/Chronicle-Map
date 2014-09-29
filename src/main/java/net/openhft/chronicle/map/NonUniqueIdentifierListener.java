package net.openhft.chronicle.map;

/**
 * @author Rob Austin.
 */
public interface NonUniqueIdentifierListener {

    void onNonUniqueIdentifier(byte remoteIdentifier);
}
