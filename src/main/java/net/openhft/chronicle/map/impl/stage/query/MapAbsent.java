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
