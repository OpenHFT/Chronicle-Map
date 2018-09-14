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
import net.openhft.chronicle.hash.impl.stage.replication.ReplicableEntryDelegating;
import net.openhft.chronicle.hash.replication.ReplicableEntry;
import net.openhft.chronicle.map.MapContext;
import net.openhft.chronicle.map.MapEntry;
import net.openhft.chronicle.map.impl.stage.entry.ReplicatedMapEntryStages;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;

@Staged
public class ReplicatedMapEntryDelegating<K, V>
        implements MapEntry<K, V>, ReplicableEntryDelegating {

    @StageRef
    ReplicatedMapSegmentIteration<K, V, ?> delegate;
    @StageRef
    ReplicatedMapEntryStages<K, V> e;

    @NotNull
    @Override
    public MapContext<K, V, ?> context() {
        return delegate.context();
    }

    @NotNull
    @Override
    public Data<K> key() {
        return delegate.key();
    }

    @NotNull
    @Override
    public Data<V> value() {
        return delegate.value();
    }

    @Override
    public void doReplaceValue(Data<V> newValue) {
        delegate.doReplaceValue(newValue);
    }

    @Override
    public void doRemove() {
        delegate.doRemove();
    }

    @Override
    public ReplicableEntry d() {
        return e;
    }
}
