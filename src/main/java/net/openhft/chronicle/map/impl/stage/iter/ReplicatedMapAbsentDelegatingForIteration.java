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

package net.openhft.chronicle.map.impl.stage.iter;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.chronicle.hash.impl.stage.replication.ReplicableEntryDelegating;
import net.openhft.chronicle.hash.replication.ReplicableEntry;
import net.openhft.chronicle.map.MapAbsentEntry;
import net.openhft.chronicle.map.impl.stage.entry.ReplicatedMapEntryStages;
import net.openhft.chronicle.map.impl.stage.map.DefaultValue;
import net.openhft.chronicle.map.impl.stage.map.WrappedValueInstanceDataHolderAccess;
import net.openhft.chronicle.set.SetAbsentEntry;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;

@Staged
public class ReplicatedMapAbsentDelegatingForIteration<K, V>
        implements MapAbsentEntry<K, V>, SetAbsentEntry<K>, ReplicableEntryDelegating {

    @StageRef
    CheckOnEachPublicOperation checkOnEachPublicOperation;
    @StageRef
    ReplicatedMapSegmentIteration<K, V, ?> delegate;
    @StageRef
    ReplicatedMapEntryStages<K, V> e;
    @StageRef
    DefaultValue<V> defaultValue;

    @NotNull
    @Override
    public WrappedValueInstanceDataHolderAccess<K, V, ?> context() {
        return delegate.context();
    }

    @Override
    public void doInsert(Data<V> value) {
        delegate.doInsert(value);
    }

    @Override
    public void doInsert() {
        delegate.doInsert();
    }

    @NotNull
    @Override
    public Data<V> defaultValue() {
        return defaultValue.defaultValue();
    }

    @NotNull
    @Override
    public Data<K> absentKey() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return e.entryKey;
    }

    @Override
    public ReplicableEntry d() {
        return e;
    }
}
