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

package net.openhft.chronicle.map.impl.stage.iter;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.stage.iter.HashSegmentIteration;
import net.openhft.chronicle.map.MapContext;
import net.openhft.chronicle.map.MapEntry;
import net.openhft.chronicle.map.impl.IterationContextInterface;
import net.openhft.chronicle.map.impl.stage.entry.MapEntryStages;
import net.openhft.chronicle.map.impl.stage.map.WrappedValueInstanceValueHolder;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;

@Staged
public abstract class MapSegmentIteration<K, V, R> extends HashSegmentIteration<K, MapEntry<K, V>>
        implements MapEntry<K, V>, IterationContextInterface<K, V, R> {
    
    @StageRef MapEntryStages<K, V> entry;
    @StageRef WrappedValueInstanceValueHolder<K, V, ?> wrappedValueInstanceValueHolder;

    @Override
    public void doReplaceValue(Data<V> newValue) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        try {
            entry.innerDefaultReplaceValue(newValue);
        } finally {
            s.innerWriteLock.unlock();
        }
    }

    @NotNull
    @Override
    public MapContext<K, V, ?> context() {
        return wrappedValueInstanceValueHolder;
    }
}
