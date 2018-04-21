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

package net.openhft.chronicle.map.impl.stage.map;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.chronicle.map.MapAbsentEntry;
import net.openhft.chronicle.map.MapContext;
import net.openhft.chronicle.map.MapEntry;
import net.openhft.chronicle.map.impl.VanillaChronicleMapHolder;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;

@Staged
public abstract class MapEntryOperationsDelegation<K, V, R> implements MapContext<K, V, R> {

    @StageRef
    CheckOnEachPublicOperation checkOnEachPublicOperation;
    @StageRef
    VanillaChronicleMapHolder<K, V, R> mh;

    @Override
    public R replaceValue(@NotNull MapEntry<K, V> entry, Data<V> newValue) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return mh.m().entryOperations.replaceValue(entry, newValue);
    }

    @Override
    public R remove(@NotNull MapEntry<K, V> entry) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return mh.m().entryOperations.remove(entry);
    }

    @Override
    public Data<V> defaultValue(@NotNull MapAbsentEntry<K, V> absentEntry) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return mh.m().defaultValueProvider.defaultValue(absentEntry);
    }

    @Override
    public R insert(@NotNull MapAbsentEntry<K, V> absentEntry, Data<V> value) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return mh.m().entryOperations.insert(absentEntry, value);
    }
}
