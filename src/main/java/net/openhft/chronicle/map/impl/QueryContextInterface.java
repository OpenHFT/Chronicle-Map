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

package net.openhft.chronicle.map.impl;

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.map.ExternalMapQueryContext;
import net.openhft.chronicle.map.impl.ret.InstanceReturnValue;
import net.openhft.chronicle.map.impl.ret.UsableReturnValue;

public interface QueryContextInterface<K, V, R> extends ExternalMapQueryContext<K, V, R> {

    void initInputKey(Data<K> inputKey);

    Data<K> getInputKeyBytesAsData(BytesStore bytesStore, long offset, long size);

    DataAccess<K> inputKeyDataAccess();

    InstanceReturnValue<V> defaultReturnValue();

    UsableReturnValue<V> usingReturnValue();

    DataAccess<V> inputValueDataAccess();

    Closeable acquireHandle();

    void initSegmentIndex(int segmentIndex);

    boolean segmentIndexInit();
}
