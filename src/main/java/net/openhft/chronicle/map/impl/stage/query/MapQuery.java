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

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.stage.query.HashQuery;
import net.openhft.chronicle.hash.impl.stage.query.KeySearch;
import net.openhft.chronicle.hash.impl.stage.query.SearchAllocatedChunks;
import net.openhft.chronicle.map.ExternalMapQueryContext;
import net.openhft.chronicle.map.MapAbsentEntry;
import net.openhft.chronicle.map.MapContext;
import net.openhft.chronicle.map.MapEntry;
import net.openhft.chronicle.map.impl.MapAbsentEntryHolder;
import net.openhft.chronicle.map.impl.QueryContextInterface;
import net.openhft.chronicle.map.impl.VanillaChronicleMapHolder;
import net.openhft.chronicle.map.impl.stage.data.bytes.InputFirstValueBytesData;
import net.openhft.chronicle.map.impl.stage.data.bytes.InputSecondValueBytesData;
import net.openhft.chronicle.map.impl.stage.data.instance.InputValueInstanceData;
import net.openhft.chronicle.map.impl.stage.entry.MapEntryStages;
import net.openhft.chronicle.map.impl.stage.ret.DefaultReturnValue;
import net.openhft.chronicle.map.impl.stage.ret.UsingReturnValue;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static net.openhft.chronicle.hash.impl.stage.query.KeySearch.SearchState.PRESENT;

@Staged
public abstract class MapQuery<K, V, R> extends HashQuery<K>
        implements MapEntry<K, V>, ExternalMapQueryContext<K, V, R>,
        QueryContextInterface<K, V, R> {

    @StageRef VanillaChronicleMapHolder<K, ?, ?, V, ?, ?, R> mh;
    @StageRef MapEntryStages<K, V> e;
    @StageRef SearchAllocatedChunks allocatedChunks;
    @StageRef KeySearch<K> ks;
    @StageRef public AcquireHandle<K, V> acquireHandle;
    
    @StageRef public InputValueInstanceData<V, ?, ?> inputValueInstanceValue;
    
    @StageRef public InputFirstValueBytesData<V> inputFirstValueBytesValue;
    @StageRef public InputSecondValueBytesData<V> inputSecondValueBytesValue;
    
    @StageRef public DefaultReturnValue<V> defaultReturnValue;
    @StageRef public UsingReturnValue<V> usingReturnValue;

    @StageRef public MapAbsentEntryHolder<K, V> absent;

    @Override
    public MapEntry<K, V> entry() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return entryPresent() ? this : null;
    }

    @Nullable
    @Override
    public MapAbsentEntry<K, V> absentEntry() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return entryPresent() ? null : absent.absent();
    }
    
    protected void putPrefix() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        boolean underUpdatedLockIsHeld = !s.innerUpdateLock.isHeldByCurrentThread();
        if (underUpdatedLockIsHeld)
            s.innerUpdateLock.lock();
        boolean searchResultsNotTrusted = underUpdatedLockIsHeld ||
                s.concurrentSameThreadContexts;
        if (hlp.hashLookupPosInit() && ks.searchStateAbsent() && searchResultsNotTrusted)
            hlp.closeHashLookupPos();
    }

    @Override
    public void doReplaceValue(Data<V> newValue) {
        putPrefix();
        if (entryPresent()) {
            e.innerDefaultReplaceValue(newValue);
            s.incrementModCount();
            ks.setSearchState(PRESENT);
            initPresenceOfEntry(EntryPresence.PRESENT);
        } else {
            throw new IllegalStateException(
                    "Entry is absent in the map when doReplaceValue() is called");
        }
    }

    @NotNull
    @Override
    public MapContext<K, V, ?> context() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return this;
    }
}
