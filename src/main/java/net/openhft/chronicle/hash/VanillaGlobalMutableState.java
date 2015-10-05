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

import net.openhft.lang.model.Byteable;

/**
 * <h1>This interface is a private API, don't use in client code</h1>
 *
 * Excluding global mutable state lock
 */
public interface VanillaGlobalMutableState extends Byteable {

    long getAllocatedExtraTierBulks();

    void setAllocatedExtraTierBulks(long allocatedExtraTierBulks);

    long getFirstFreeTierIndex();

    void setFirstFreeTierIndex(long firstFreeTierIndex);

    long getExtraTiersInUse();
    void setExtraTiersInUse(long extraTiersInUse);

    long getSegmentHeadersOffset();
    void setSegmentHeadersOffset(long segmentHeadersOffset);
}
