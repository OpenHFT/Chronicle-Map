/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
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

package net.openhft.chronicle.hash.impl.stage.entry;

public enum NoChecksumStrategy implements ChecksumStrategy {
    INSTANCE;

    @Override
    public void computeAndStoreChecksum() {
        throw new UnsupportedOperationException("Checksum is not stored in this Chronicle Hash");
    }

    @Override
    public boolean innerCheckSum() {
        return true;
    }

    @Override
    public int computeChecksum() {
        return 0;
    }

    @Override
    public int storedChecksum() {
        return 0;
    }

    @Override
    public long extraEntryBytes() {
        return 0; // no extra bytes to store checksum
    }
}
