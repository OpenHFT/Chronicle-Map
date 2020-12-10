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

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.chronicle.hash.impl.util.Objects;
import net.openhft.chronicle.map.MapContext;
import net.openhft.chronicle.map.impl.stage.data.bytes.WrappedValueBytesData;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public abstract class WrappedValueBytesDataAccess<K, V, R> implements MapContext<K, V, R> {

    @StageRef
    CheckOnEachPublicOperation checkOnEachPublicOperation;
    @StageRef
    WrappedValueBytesData<V> wrappedValueBytesData;

    @Override
    public Data<V> wrapValueBytesAsData(BytesStore bytesStore, long offset, long size) {
        Objects.requireNonNull(bytesStore);
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        WrappedValueBytesData<V> wrapped = this.wrappedValueBytesData;
        wrapped = wrapped.getUnusedWrappedValueBytesData();
        wrapped.initWrappedValueBytesStore(bytesStore, offset, size);
        return wrapped;
    }
}
