/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.hash.replication;

import java.net.SocketAddress;

/**
 * @author Rob Austin.
 */
public interface RemoteNodeValidator {

    /**
     * checks the identifier that is unique and we haven't seen it before, unless it comes from the same port
     * and host.
     *
     * @param remoteIdentifier remoteIdentifier
     * @param remoteAddress    remoteAddress
     * @return true if unique
     */
    boolean validate(byte remoteIdentifier, SocketAddress remoteAddress);

}
