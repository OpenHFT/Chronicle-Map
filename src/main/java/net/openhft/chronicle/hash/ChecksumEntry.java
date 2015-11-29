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

package net.openhft.chronicle.hash;

import net.openhft.chronicle.map.MapAbsentEntry;
import net.openhft.chronicle.map.MapEntry;

/**
 * Abstracts entries of hash containers, created by {@link ChronicleHashBuilder}s
 * with {@link ChronicleHashBuilder#checksumEntries(boolean)} configured to {@code true}.
 */
public interface ChecksumEntry {

    /**
     * Re-computes and stores checksum for the entry. This method <i>shouldn't</i> be called before
     * or after ordinary operations like {@link MapAbsentEntry#doInsert(Data)}, {@link
     * MapEntry#doReplaceValue(Data)}: it is performed automatically underneath. Call this method,
     * only when value bytes was updated directly, for example though flyweight implementation of
     * a <a href="https://github.com/OpenHFT/Chronicle-Values#value-interface-specification">value
     * interface</a>.
     */
    void updateChecksum();

    /**
     * Computes checksum from the entry bytes and checks whether it is equal to the stored checksum.
     *
     * @return {@code true} if stored checksum equals to checksum computed from the entry bytes
     */
    boolean checkSum();
}
