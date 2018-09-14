/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map.impl.stage.query;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.stage.entry.HashLookupSearch;
import net.openhft.chronicle.hash.impl.stage.entry.SegmentStages;
import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.chronicle.hash.impl.stage.query.HashQuery.EntryPresence;
import net.openhft.chronicle.hash.impl.stage.query.KeySearch;
import net.openhft.chronicle.map.impl.VanillaChronicleMapHolder;
import net.openhft.chronicle.map.impl.stage.entry.MapEntryStages;
import net.openhft.chronicle.set.DummyValueData;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;

import static net.openhft.chronicle.hash.impl.stage.query.KeySearch.SearchState.PRESENT;

@Staged
public abstract class MapAbsent<K, V> implements Absent<K, V> {

    @StageRef
    public KeySearch<K> ks;
    @StageRef
    public HashLookupSearch hashLookupSearch;
    @StageRef
    public CheckOnEachPublicOperation checkOnEachPublicOperation;
    @StageRef
    public SegmentStages s;
    @StageRef
    MapQuery<K, V, ?> q;
    @StageRef
    MapEntryStages<K, V> e;
    @StageRef
    VanillaChronicleMapHolder<K, V, ?> mh;

    void putEntry(Data<V> value) {
        assert ks.searchStateAbsent();
        long entrySize = e.entrySize(ks.inputKey.size(), value.size());
        q.allocatedChunks.initEntryAndKey(entrySize);
        e.initValue(value);
        e.freeExtraAllocatedChunks();
        hashLookupSearch.putNewVolatile(e.pos);
    }

    @NotNull
    @Override
    public MapQuery<K, V, ?> context() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return q;
    }

    @NotNull
    @Override
    public Data<K> absentKey() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return ks.inputKey;
    }

    @Override
    public void doInsert(Data<V> value) {
        q.putPrefix();
        if (!q.entryPresent()) {
            putEntry(value);
            s.incrementModCount();
            ks.setSearchState(PRESENT);
            q.initPresenceOfEntry(EntryPresence.PRESENT);
        } else {
            throw new IllegalStateException(mh.h().toIdentityString() +
                    ": Entry is present in the map when doInsert() is called");
        }
    }

    @Override
    public void doInsert() {
        if (mh.set() == null)
            throw new IllegalStateException(mh.h().toIdentityString() +
                    ": Called SetAbsentEntry.doInsert() from Map context");
        //noinspection unchecked
        doInsert((Data<V>) DummyValueData.INSTANCE);
    }
}
