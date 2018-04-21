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
import net.openhft.chronicle.hash.ReplicatedHashSegmentContext;
import net.openhft.chronicle.hash.replication.ReplicableEntry;
import net.openhft.chronicle.map.MapEntry;
import net.openhft.chronicle.map.impl.ReplicatedIterationContext;
import net.openhft.chronicle.map.impl.VanillaChronicleMapHolder;
import net.openhft.chronicle.map.impl.stage.data.DummyValueZeroData;
import net.openhft.chronicle.map.impl.stage.entry.ReplicatedMapEntryStages;
import net.openhft.chronicle.map.impl.stage.replication.ReplicationUpdate;
import net.openhft.chronicle.set.DummyValueData;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

import java.util.function.Consumer;
import java.util.function.Predicate;

import static net.openhft.chronicle.map.impl.stage.iter.ReplicatedMapSegmentIteration.EntriesToTest.ALL;
import static net.openhft.chronicle.map.impl.stage.iter.ReplicatedMapSegmentIteration.EntriesToTest.PRESENT;

@Staged
public abstract class ReplicatedMapSegmentIteration<K, V, R> extends MapSegmentIteration<K, V, R>
        implements ReplicatedIterationContext<K, V, R>, ReplicableEntry,
        ReplicatedHashSegmentContext<K, MapEntry<K, V>> {

    @StageRef
    VanillaChronicleMapHolder<K, V, R> mh;
    @StageRef
    ReplicatedMapEntryStages<K, V> e;
    @StageRef
    ReplicationUpdate<K> ru;
    @StageRef
    DummyValueZeroData<V> dummyValue;
    @StageRef
    ReplicatedMapAbsentDelegatingForIteration<K, V> absentEntryDelegating;
    @StageRef
    ReplicatedMapEntryDelegating<K, V> entryDelegating;
    EntriesToTest entriesToTest = null;

    void initEntriesToTest(EntriesToTest entriesToTest) {
        this.entriesToTest = entriesToTest;
    }

    @Override
    public boolean shouldTestEntry() {
        return entriesToTest == ALL || !e.entryDeleted();
    }

    @Override
    public Object entryForIteration() {
        return !e.entryDeleted() ? entryDelegating : absentEntryDelegating;
    }

    @Override
    public long tierEntriesForIteration() {
        return entriesToTest == ALL ? s.tierEntries() : s.tierEntries() - s.tierDeleted();
    }

    @Override
    public void doReplaceValue(Data<V> newValue) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        try {
            entry.innerDefaultReplaceValue(newValue);
            e.updatedReplicationStateOnPresentEntry();
            ru.updateChange();
        } finally {
            s.innerWriteLock.unlock();
        }
    }

    @Override
    public boolean forEachSegmentEntryWhile(Predicate<? super MapEntry<K, V>> predicate) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        initEntriesToTest(PRESENT);
        s.innerUpdateLock.lock();
        return innerForEachSegmentEntryWhile(predicate);
    }

    @Override
    public boolean forEachSegmentReplicableEntryWhile(
            Predicate<? super ReplicableEntry> predicate) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        initEntriesToTest(ALL);
        s.innerUpdateLock.lock();
        return innerForEachSegmentEntryWhile(predicate);
    }

    @Override
    public void forEachSegmentReplicableEntry(Consumer<? super ReplicableEntry> action) {
        forEachSegmentReplicableEntryWhile(e -> {
            action.accept(e);
            return true;
        });
    }

    @Override
    public void doRemove() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        try {
            if (e.valueSize > dummyValue.size())
                e.innerDefaultReplaceValue(dummyValue);
            e.updatedReplicationStateOnPresentEntry();
            e.writeEntryDeleted();
            ru.updateChange();
            s.tierDeleted(s.tierDeleted() + 1);
        } finally {
            s.innerWriteLock.unlock();
        }
        initEntryRemovedOnThisIteration(true);
    }

    @Override
    public void doRemoveCompletely() {
        boolean wasDeleted = e.entryDeleted();
        super.doRemove();
        ru.dropChange();
        if (wasDeleted)
            s.tierDeleted(s.tierDeleted() - 1);
    }

    public void doInsert(Data<V> value) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        if (e.entryDeleted()) {
            try {
                s.tierDeleted(s.tierDeleted() - 1);
                e.innerDefaultReplaceValue(value);
                s.incrementModCount();
                e.writeEntryPresent();
                e.updatedReplicationStateOnPresentEntry();
                ru.updateChange();
            } finally {
                s.innerWriteLock.unlock();
            }
        } else {
            throw new IllegalStateException(mh.h().toIdentityString() +
                    ": Entry is present in the map when doInsert() is called");
        }
    }

    public void doInsert() {
        if (mh.set() == null)
            throw new IllegalStateException(mh.h().toIdentityString() +
                    ": Called SetAbsentEntry.doInsert() from Map context");
        doInsert((Data<V>) DummyValueData.INSTANCE);
    }

    enum EntriesToTest {PRESENT, ALL}
}
