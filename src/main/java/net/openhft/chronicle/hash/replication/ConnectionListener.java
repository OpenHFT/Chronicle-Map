/*
 *      Copyright (C) 2015  higherfrequencytrading.com
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

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