package net.openhft.chronicle.map.impl.stage.iter;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.replication.ReplicableEntry;
import net.openhft.chronicle.map.impl.ReplicatedIterationContextInterface;
import net.openhft.chronicle.map.impl.stage.data.DummyValueZeroData;
import net.openhft.chronicle.map.impl.stage.entry.ReplicatedMapEntryStages;
import net.openhft.chronicle.map.impl.stage.replication.ReplicationUpdate;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

import java.util.function.Consumer;

@Staged
public abstract class ReplicatedMapSegmentIteration<K, V, R> extends MapSegmentIteration<K, V, R>
        implements ReplicatedIterationContextInterface<K, V, R>, ReplicableEntry {

    @StageRef ReplicatedMapEntryStages<K, V, ?> e;
    @StageRef ReplicationUpdate<K> ru;
    @StageRef DummyValueZeroData<V> dummyValue;

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
    public void forEachReplicableEntry(Consumer<? super ReplicableEntry> action) {
        s.innerUpdateLock.lock();
        try {
            long entries = s.entries();
            if (entries == 0)
                return;
            long startPos = 0L;
            while (!hashLookup.empty(hashLookup.readEntry(startPos))) {
                startPos = hashLookup.step(startPos);
            }
            hlp.initHashLookupPos(startPos);
            do {
                hlp.setHashLookupPos(hashLookup.step(hlp.hashLookupPos));
                long entry = hashLookup.readEntry(hlp.hashLookupPos);
                if (!hashLookup.empty(entry)) {
                    e.readExistingEntry(hashLookup.value(entry));
                    action.accept(this);
                    if (--entries == 0)
                        break;
                }
            } while (hlp.hashLookupPos != startPos);
        } finally {
            s.innerReadLock.unlock();
        }
    }



    @Override
    public void doRemove() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        initEntryRemovedOnThisIteration(true);
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
    }
}
