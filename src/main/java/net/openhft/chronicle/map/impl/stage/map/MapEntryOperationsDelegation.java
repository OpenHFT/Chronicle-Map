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

package net.openhft.chronicle.map.impl.stage.map;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.chronicle.map.MapAbsentEntry;
import net.openhft.chronicle.map.MapContext;
import net.openhft.chronicle.map.MapEntry;
import net.openhft.chronicle.map.impl.VanillaChronicleMapHolder;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;

@Staged
public abstract class MapEntryOperationsDelegation<K, V, R> implements MapContext<K, V, R> {

    @StageRef
    CheckOnEachPublicOperation checkOnEachPublicOperation;
    @StageRef
    VanillaChronicleMapHolder<K, V, R> mh;

    @Override
    public R replaceValue(@NotNull MapEntry<K, V> entry, Data<V> newValue) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return mh.m().entryOperations.replaceValue(entry, newValue);
    }

    @Override
    public R remove(@NotNull MapEntry<K, V> entry) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return mh.m().entryOperations.remove(entry);
    }

    @Override
    public Data<V> defaultValue(@NotNull MapAbsentEntry<K, V> absentEntry) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return mh.m().defaultValueProvider.defaultValue(absentEntry);
    }

    @Override
    public R insert(@NotNull MapAbsentEntry<K, V> absentEntry, Data<V> value) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return mh.m().entryOperations.insert(absentEntry, value);
    }
}
