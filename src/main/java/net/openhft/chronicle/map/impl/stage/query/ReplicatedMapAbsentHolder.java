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

package net.openhft.chronicle.map.impl.stage.query;

import net.openhft.chronicle.map.MapAbsentEntry;
import net.openhft.chronicle.map.impl.MapAbsentEntryHolder;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public class ReplicatedMapAbsentHolder<K, V> implements MapAbsentEntryHolder<K, V> {

    @StageRef ReplicatedMapAbsent<K, V> ab;

    @Override
    public MapAbsentEntry<K, V> absent() {
        return ab;
    }
}
