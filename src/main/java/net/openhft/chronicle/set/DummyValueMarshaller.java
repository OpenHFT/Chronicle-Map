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

package net.openhft.chronicle.set;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.impl.EnumMarshallable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

final class DummyValueMarshaller implements DataAccess<DummyValue>, SizedReader<DummyValue>,
        EnumMarshallable<DummyValueMarshaller> {
    public static final DummyValueMarshaller INSTANCE = new DummyValueMarshaller();

    private DummyValueMarshaller() {
    }

    @Override
    public Data<DummyValue> getData(@NotNull DummyValue instance) {
        return DummyValueData.INSTANCE;
    }

    @Override
    public void uninit() {
        // no op
    }

    @Override
    public DataAccess<DummyValue> copy() {
        return this;
    }

    @NotNull
    @Override
    public DummyValue read(@NotNull Bytes in, long size, @Nullable DummyValue using) {
        return DummyValue.DUMMY_VALUE;
    }

    @Override
    public DummyValueMarshaller readResolve() {
        return INSTANCE;
    }
}

