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

package net.openhft.chronicle.map.impl.stage.input;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.stage.entry.SegmentStages;
import net.openhft.chronicle.hash.impl.stage.hash.LogHolder;
import net.openhft.chronicle.hash.impl.stage.query.KeySearch;
import net.openhft.chronicle.hash.replication.RemoteOperationContext;
import net.openhft.chronicle.map.impl.ReplicatedChronicleMapHolder;
import net.openhft.chronicle.map.impl.stage.data.DummyValueZeroData;
import net.openhft.chronicle.map.impl.stage.data.bytes.ReplicatedInputKeyBytesData;
import net.openhft.chronicle.map.impl.stage.data.bytes.ReplicatedInputValueBytesData;
import net.openhft.chronicle.map.impl.stage.entry.ReplicatedMapEntryStages;
import net.openhft.chronicle.map.impl.stage.query.ReplicatedMapQuery;
import net.openhft.chronicle.map.impl.stage.replication.ReplicationUpdate;
import net.openhft.chronicle.map.replication.MapRemoteQueryContext;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;


@Staged
public abstract class ReplicatedInput<K, V, R>
        implements RemoteOperationContext<K>, MapRemoteQueryContext<K, V, R> {
    
    @StageRef ReplicatedChronicleMapHolder<K, V, R> mh;
    @StageRef ReplicationUpdate<K> ru;
    @StageRef LogHolder lh;
    @StageRef ReplicatedInputKeyBytesData<K> replicatedInputKeyBytesValue;
    @StageRef ReplicatedInputValueBytesData<V> replicatedInputValueBytesValue;
    @StageRef KeySearch<?> ks;
    @StageRef ReplicatedMapQuery<K, V, ?> q;
    @StageRef SegmentStages s;
    @StageRef ReplicatedMapEntryStages<K, V> e;
    @StageRef DummyValueZeroData<V> dummyValue;

    @Override
    public Data<V> dummyZeroValue() {
        return dummyValue;
    }

    public Bytes replicatedInputBytes = null;

    public void initReplicatedInputBytes(Bytes replicatedInputBytes) {
        this.replicatedInputBytes = replicatedInputBytes;
    }

    // ri for "replication input"
    @Stage("ReplicationInput") public long bootstrapTimestamp;
    @Stage("ReplicationInput") public long riKeySize = -1;
    @Stage("ReplicationInput") public long riValueSize;

    @Stage("ReplicationInput") public long riKeyOffset;
    @Stage("ReplicationInput") public long riValueOffset;

    @Stage("ReplicationInput") public long riTimestamp;
    @Stage("ReplicationInput") public byte riId;
    @Stage("ReplicationInput") public boolean isDeleted;


    public void initReplicationInput(Bytes replicatedInputBytes) {
        initReplicatedInputBytes(replicatedInputBytes);
        bootstrapTimestamp = replicatedInputBytes.readLong();
        riKeySize = mh.m().keySizeMarshaller.readSize(replicatedInputBytes);
        riValueSize = mh.m().valueSizeMarshaller.readSize(replicatedInputBytes);

        riTimestamp = replicatedInputBytes.readStopBit();
        riId = replicatedInputBytes.readByte();
        ru.initReplicationUpdate(riTimestamp, riId);

        isDeleted = replicatedInputBytes.readBoolean();

        riKeyOffset = replicatedInputBytes.readPosition();
        riValueOffset = riKeyOffset + riKeySize;
    }

    public void processReplicatedEvent() {

        mh.m().setLastModificationTime(riId, bootstrapTimestamp);

        q.initInputKey(replicatedInputKeyBytesValue);

        boolean debugEnabled = lh.LOG.isDebugEnabled();

        s.innerUpdateLock.lock();
        if (isDeleted) {
            if (debugEnabled) {
                lh.LOG.debug("READING FROM SOURCE -  into local-id={}, remote={}, remove(key={})",
                        mh.m().identifier(), riId, ks.inputKey);
            }
            mh.m().remoteOperations.remove(this);
            return;
        }

        String message = null;
        if (debugEnabled) {
            message = String.format(
                    "READING FROM SOURCE -  into local-id=%d, remote-id=%d, put(key=%s,",
                    mh.m().identifier(), riId, ks.inputKey);
        }


        mh.m().remoteOperations.put(this, replicatedInputValueBytesValue);


        if (debugEnabled) {
            lh.LOG.debug(message + "value=" + replicatedInputValueBytesValue + ")");
        }
    }
}
