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

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.VanillaGlobalMutableState;
import net.openhft.chronicle.values.Array;
import net.openhft.chronicle.values.Group;

interface ReplicatedGlobalMutableState extends VanillaGlobalMutableState {

    @Group(5)
    int getCurrentCleanupSegmentIndex();
    void setCurrentCleanupSegmentIndex(int currentCleanupSegmentIndex);

    @Group(6)
    @Array(length = 128)
    boolean getModificationIteratorInitAt(int index);
    void setModificationIteratorInitAt(int index, boolean init);
}
