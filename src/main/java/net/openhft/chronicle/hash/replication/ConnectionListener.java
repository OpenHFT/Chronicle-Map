package net.openhft.chronicle.hash.replication;

import java.net.SocketAddress;

public interface ConnectionListener {

    /**
     * @param address    the address that we are connceted to
     * @param identifier the remote identifier that we are not connected to.
     * @param isServer   if this host accepted the connection {@code isServer} is {@code true}, if
     *                   we made the connection to a remote host then {@code isServer} is {@code
     *                   false}
     */
    void onConnect(SocketAddress address, byte identifier, boolean isServer);


    /**
     * @param address the address that we have been disconnected from
     * @param identifier the identifer the address that we have been disconnected from or Byte
     *                   .MIN_VALUE if not known
     */
    void onDisconnect(SocketAddress address, byte identifier);


}