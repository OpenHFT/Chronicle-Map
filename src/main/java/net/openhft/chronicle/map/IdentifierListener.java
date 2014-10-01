package net.openhft.chronicle.map;

import java.net.SocketAddress;

/**
 * @author Rob Austin.
 */
public interface IdentifierListener {

    /**
     * checks the identifier that is unique and we haven't see it before, unless it comes from the same port
     * and host.
     *
     * @param remoteIdentifier
     * @param remoteAddress
     * @return
     */
    boolean isIdentifierUnique(byte remoteIdentifier, SocketAddress remoteAddress);

}
