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

package net.openhft.chronicle.hash.impl;

import net.openhft.lang.io.MultiStoreBytes;

/**
 * This awful class is made for "conversion" from new ch.bytes.Bytes to old lang.io.Bytes
 *
 * TODO remove when Bytes system stabilize
 */
public class PublicMultiStoreBytes extends MultiStoreBytes {
    public void startAddr(long startAddr) { this.startAddr = startAddr; }
    public void positionAddr(long positionAddr) { this.positionAddr = positionAddr; }
    public void limitAddr(long limitAddr) { this.limitAddr = limitAddr; }
    public void capacityAddr(long capacityAddr) { this.capacityAddr = capacityAddr; }
}
