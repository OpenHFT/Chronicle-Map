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

package net.openhft.chronicle.hash;

import net.openhft.chronicle.bytes.Byteable;
import net.openhft.chronicle.values.Group;
import net.openhft.chronicle.values.Range;
import net.openhft.chronicle.values.Values;

/**
 * <h1>This interface is a private API, don't use in client code</h1>
 * <p>
 * Excluding global mutable state lock
 */
public interface VanillaGlobalMutableState extends Byteable {

    static void main(String[] args) {
        System.setProperty("chronicle.values.dumpCode", "true");
        Values.nativeClassFor(VanillaGlobalMutableState.class);
    }

    @Group(1)
    int getAllocatedExtraTierBulks();

    void setAllocatedExtraTierBulks(@Range(min = 0, max = 16777215) int allocatedExtraTierBulks);

    @Group(2)
    long getFirstFreeTierIndex();

    void setFirstFreeTierIndex(@Range(min = 0, max = 1099511627775L) long firstFreeTierIndex);

    @Group(3)
    long getExtraTiersInUse();

    void setExtraTiersInUse(@Range(min = 0, max = 1099511627775L) long extraTiersInUse);

    @Group(4)
    long getSegmentHeadersOffset();

    void setSegmentHeadersOffset(@Range(min = 0, max = 4294967295L) long segmentHeadersOffset);

    @Group(5)
    long getDataStoreSize();

    void setDataStoreSize(@Range(min = 0, max = Long.MAX_VALUE) long dataStoreSize);

    long addDataStoreSize(long addition);
}
