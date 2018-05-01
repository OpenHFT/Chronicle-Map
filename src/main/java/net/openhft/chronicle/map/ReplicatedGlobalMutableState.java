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

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.VanillaGlobalMutableState;
import net.openhft.chronicle.values.*;

interface ReplicatedGlobalMutableState extends VanillaGlobalMutableState {

    static void main(String[] args) {
        System.setProperty("chronicle.values.dumpCode", "true");
        Values.nativeClassFor(ReplicatedGlobalMutableState.class);
    }

    @Group(6)
    int getCurrentCleanupSegmentIndex();

    void setCurrentCleanupSegmentIndex(
            @Range(min = 0, max = Integer.MAX_VALUE) int currentCleanupSegmentIndex);

    @Group(7)
    @Align(offset = 1)
    int getModificationIteratorsCount();

    int addModificationIteratorsCount(@Range(min = 0, max = 128) int addition);

    @Group(8)
    // Align to prohibit array filling the highest bit of the previous 2-byte block, that
    // complicates generated code for all fields, defined in this interface.
    @Align(offset = 2)
    @Array(length = 128)
    boolean getModificationIteratorInitAt(int index);

    void setModificationIteratorInitAt(int index, boolean init);
}
