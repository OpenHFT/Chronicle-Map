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

package net.openhft.chronicle.map.impl.stage.iter;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.ReplicatedHashSegmentContext;
import net.openhft.chronicle.hash.impl.CompactOffHeapLinearHashTable;
import net.openhft.chronicle.hash.impl.VanillaChronicleHashHolder;
import net.openhft.chronicle.hash.replication.ReplicableEntry;
import net.openhft.chronicle.map.MapAbsentEntry;
import net.openhft.chronicle.map.MapEntry;
import net.openhft.chronicle.map.impl.ReplicatedIterationContextInterface;
import net.openhft.chronicle.map.impl.stage.data.DummyValueZeroData;
import net.openhft.chronicle.map.impl.stage.entry.ReplicatedMapEntryStages;
import net.openhft.chronicle.map.impl.stage.replication.ReplicationUpdate;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

import java.util.function.Consumer;
import java.util.function.Predicate;

@Staged
public abstract class ReplicatedMapSegmentIteration<K, V, R> extends MapSegmentIteration<K, V, R>
        implements ReplicatedIterationContextInterface<K, V, R>, ReplicableEntry,
        ReplicatedHashSegmentContext<K, MapEntry<K, V>>, MapAbsentEntry<K, V> {

    @StageRef VanillaChronicleHashHolder<?, ?, ?> hh;
    @StageRef ReplicatedMapEntryStages<K, V, ?> e;
    @StageRef ReplicationUpdate<K> ru;
    @StageRef DummyValueZeroData<V> dummyValue;
    @StageRef ReplicatedMapAbsentDelegatingForIteration<K, V> absentEntryDelegating;
    @StageRef ReplicatedMapEntryDelegating<K, V> entryDelegating;

    @Override
    public boolean entryIsPresent() {
        return !e.entryDeleted();
    }

    @Override
    public void doReplaceValue(Data<V> newValue) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        try {
            entry.innerDefaultReplaceValue(newValue);
            ru.updateChange();
            e.updatedReplicationStateOnPresentEntry();
        } finally {
            s.innerWriteLock.unlock();
        }
    }

    @Override
    public boolean forEachSegmentReplicableEntryWhile(
            Predicate<? super ReplicableEntry> predicate) {
        s.innerUpdateLock.lock();
        try {
            long entries = s.entries();
            if (entries == 0)
                return true;
            boolean interrupted = false;
            long startPos = 0L;
            CompactOffHeapLinearHashTable hashLookup = hh.h().hashLookup;
            while (!hashLookup.empty(hashLookup.readEntry(s.segmentBaseAddr, startPos))) {
                startPos = hashLookup.step(startPos);
            }
            hlp.initHashLookupPos(startPos);
            do {
                hlp.setHashLookupPos(hashLookup.step(hlp.hashLookupPos));
                long entry = hashLookup.readEntry(s.segmentBaseAddr, hlp.hashLookupPos);
                if (!hashLookup.empty(entry)) {
                    e.readExistingEntry(hashLookup.value(entry));
                    ReplicableEntry e = !this.e.entryDeleted() ? entryDelegating :
                            absentEntryDelegating;
                    initEntryRemovedOnThisIteration(false);
                    if (!predicate.test(e)) {
                        interrupted = true;
                        break;
                    } else {
                        if (--entries == 0)
                            break;
                    }
                }
            } while (hlp.hashLookupPos != startPos);
            return !interrupted;
        } finally {
            s.innerReadLock.unlock();
            initEntryRemovedOnThisIteration(false);
        }
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
            s.deleted(s.deleted() + 1);
        } finally {
            s.innerWriteLock.unlock();
        }
        initEntryRemovedOnThisIteration(true);
    }

    @Override
    public void doRemoveCompletely() {
        boolean wasDeleted = e.entryDeleted();
        super.doRemove();
        if (wasDeleted)
            s.deleted(s.deleted() - 1);
    }

    @Override
    public void doInsert(Data<V> value) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        if (e.entryDeleted()) {
            try {
                e.innerDefaultReplaceValue(value);
                s.deleted(s.deleted() - 1);
                s.incrementModCount();
                e.writeEntryPresent();
                ru.updateChange();
                e.updatedReplicationStateOnPresentEntry();
            } finally {
                s.innerWriteLock.unlock();
            }
        } else {
            throw new IllegalStateException(
                    "Entry is present in the map when doInsert() is called");
        }
    }
}
