/*
 * Copyright 2015 Higher Frequency Trading
 *
 *  http://www.higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package net.openhft.chronicle.map.impl.stage.query;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.map.MapAbsentEntry;
import net.openhft.chronicle.map.impl.stage.data.DummyValueZeroData;
import net.openhft.chronicle.map.impl.stage.entry.ReplicatedMapEntryStages;
import net.openhft.chronicle.map.impl.stage.replication.ReplicationUpdate;
import net.openhft.chronicle.map.impl.stage.ret.BytesReturnValue;
import net.openhft.chronicle.map.replication.MapRemoteQueryContext;
import net.openhft.chronicle.map.replication.MapReplicableEntry;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.Nullable;

@Staged
public abstract class ReplicatedMapQuery<K, V, R> extends MapQuery<K, V, R>
        implements MapRemoteQueryContext<K, V, R> {

    @StageRef ReplicatedMapEntryStages<K, V, ?> e;
    @StageRef ReplicationUpdate ru;

    @StageRef BytesReturnValue<V> bytesReturnValue;

    @StageRef ReplicatedMapAbsentDelegating<K, V> absentDelegating;

    @Nullable
    @Override
    public MapAbsentEntry<K, V> absentEntry() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        if (entryPresent()) {
            return null;
        } else {
            if (!searchStatePresent()) {
                return absentDelegating;
            } else {
                assert e.entryDeleted();
                return absent.absent();
            }
        }
    }

    @Override
    public boolean entryPresent() {
        return super.entryPresent() && !e.entryDeleted();
    }
    
    @StageRef DummyValueZeroData<V> dummyValue;

    @Override
    public MapReplicableEntry<K, V> entry() {
        return (MapReplicableEntry<K, V>) super.entry();
    }

    @Override
    public void doRemove() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        s.innerUpdateLock.lock();
        if (entryPresent()) {
            if (e.valueSize > dummyValue.size())
                e.innerDefaultReplaceValue(dummyValue);
            e.updatedReplicationStateOnPresentEntry();
            e.writeEntryDeleted();
            ru.updateChange();
            s.deleted(s.deleted() + 1);
        } else {
            throw new IllegalStateException("Entry is absent in the map when doRemove() is called");
        }
    }

    @Override
    public void doReplaceValue(Data<V> newValue) {
        super.doReplaceValue(newValue);
        ru.updateChange();
        e.updatedReplicationStateOnPresentEntry();
    }
}
