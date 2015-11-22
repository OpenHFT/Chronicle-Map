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

import net.openhft.chronicle.bytes.Byteable;
import net.openhft.chronicle.values.Group;

/**
 * <h1>This interface is a private API, don't use in client code</h1>
 *
 * Excluding global mutable state lock
 */
public interface VanillaGlobalMutableState extends Byteable {

    @Group(1)
    int getAllocatedExtraTierBulks();
    void setAllocatedExtraTierBulks(int allocatedExtraTierBulks);

    @Group(2)
    long getFirstFreeTierIndex();
    void setFirstFreeTierIndex(long firstFreeTierIndex);

    @Group(3)
    long getExtraTiersInUse();
    void setExtraTiersInUse(long extraTiersInUse);

    @Group(4)
    long getSegmentHeadersOffset();
    void setSegmentHeadersOffset(long segmentHeadersOffset);
}
