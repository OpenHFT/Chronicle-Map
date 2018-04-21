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

package net.openhft.chronicle.map.impl.stage.iter;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.stage.iter.HashSegmentIteration;
import net.openhft.chronicle.map.MapEntry;
import net.openhft.chronicle.map.impl.IterationContext;
import net.openhft.chronicle.map.impl.stage.data.instance.WrappedValueInstanceDataHolder;
import net.openhft.chronicle.map.impl.stage.entry.MapEntryStages;
import net.openhft.chronicle.map.impl.stage.map.WrappedValueInstanceDataHolderAccess;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;

@Staged
public abstract class MapSegmentIteration<K, V, R> extends HashSegmentIteration<K, MapEntry<K, V>>
        implements MapEntry<K, V>, IterationContext<K, V, R> {

    @StageRef
    MapEntryStages<K, V> entry;
    @StageRef
    WrappedValueInstanceDataHolder<V> wrappedValueInstanceDataHolder;
    @StageRef
    WrappedValueInstanceDataHolderAccess<K, V, ?> wrappedValueInstanceDataHolderAccess;

    @Override
    public void hookAfterEachIteration() {
        wrappedValueInstanceDataHolder.closeValue();
    }

    @Override
    public void doReplaceValue(Data<V> newValue) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        try {
            entry.innerDefaultReplaceValue(newValue);
        } finally {
            s.innerWriteLock.unlock();
        }
    }

    @NotNull
    @Override
    public WrappedValueInstanceDataHolderAccess<K, V, ?> context() {
        return wrappedValueInstanceDataHolderAccess;
    }
}
