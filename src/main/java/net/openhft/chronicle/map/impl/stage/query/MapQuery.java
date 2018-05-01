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

package net.openhft.chronicle.map.impl.stage.query;

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.stage.data.bytes.InputKeyBytesData;
import net.openhft.chronicle.hash.impl.stage.query.HashQuery;
import net.openhft.chronicle.hash.impl.stage.query.KeySearch;
import net.openhft.chronicle.hash.impl.stage.query.SearchAllocatedChunks;
import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.map.ExternalMapQueryContext;
import net.openhft.chronicle.map.MapEntry;
import net.openhft.chronicle.map.impl.QueryContextInterface;
import net.openhft.chronicle.map.impl.VanillaChronicleMapHolder;
import net.openhft.chronicle.map.impl.stage.entry.MapEntryStages;
import net.openhft.chronicle.map.impl.stage.ret.DefaultReturnValue;
import net.openhft.chronicle.map.impl.stage.ret.UsingReturnValue;
import net.openhft.chronicle.set.ExternalSetQueryContext;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static net.openhft.chronicle.hash.impl.stage.query.KeySearch.SearchState.PRESENT;

@Staged
public abstract class MapQuery<K, V, R> extends HashQuery<K>
        implements MapEntry<K, V>, ExternalMapQueryContext<K, V, R>,
        ExternalSetQueryContext<K, R>, QueryContextInterface<K, V, R>, MapAndSetContext<K, V, R> {

    @StageRef
    public AcquireHandle<K, V> acquireHandle;
    @StageRef
    public DefaultReturnValue<V> defaultReturnValue;
    @StageRef
    public UsingReturnValue<V> usingReturnValue;
    @StageRef
    public MapAbsent<K, V> absent;
    @StageRef
    VanillaChronicleMapHolder<K, V, R> mh;
    final DataAccess<V> innerInputValueDataAccess = mh.m().valueDataAccess.copy();
    @StageRef
    MapEntryStages<K, V> e;
    @StageRef
    SearchAllocatedChunks allocatedChunks;
    @StageRef
    KeySearch<K> ks;
    @StageRef
    InputKeyBytesData<K> inputKeyBytesData;
    /**
     * Same as {@link #inputKeyDataAccessInitialized}
     */
    @Stage("InputValueDataAccess")
    private boolean inputValueDataAccessInitialized = false;

    void initInputValueDataAccess() {
        inputValueDataAccessInitialized = true;
    }

    void closeInputValueDataAccess() {
        innerInputValueDataAccess.uninit();
        inputValueDataAccessInitialized = false;
    }

    @Override
    public DataAccess<V> inputValueDataAccess() {
        initInputValueDataAccess();
        return innerInputValueDataAccess;
    }

    @Override
    public MapQuery<K, V, R> entry() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return entryPresent() ? this : null;
    }

    @Nullable
    @Override
    public Absent<K, V> absentEntry() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return entryPresent() ? null : absent;
    }

    protected void putPrefix() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        if (!s.innerUpdateLock.isHeldByCurrentThread())
            s.innerUpdateLock.lock();
        if (s.nestedContextsLockedOnSameSegment &&
                s.rootContextLockedOnThisSegment.latestSameThreadSegmentModCount() !=
                        s.contextModCount) {
            if (hlp.hashLookupPosInit() && ks.searchStateAbsent())
                hlp.closeHashLookupPos();
        }
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
            throw new IllegalStateException(mh.h().toIdentityString() +
                    ": Entry is absent in the map when doReplaceValue() is called");
        }
    }

    @NotNull
    @Override
    public MapQuery<K, V, R> context() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return this;
    }

    @Override
    public Data<K> getInputKeyBytesAsData(BytesStore bytesStore, long offset, long size) {
        inputKeyBytesData.initInputKeyBytesStore(bytesStore, offset, size);
        return inputKeyBytesData;
    }
}
