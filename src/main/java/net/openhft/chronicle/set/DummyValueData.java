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

import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.hash.AbstractData;
import net.openhft.chronicle.map.impl.stage.data.ZeroBytesStore;
import org.jetbrains.annotations.Nullable;

public class DummyValueData extends AbstractData<DummyValue> {

    public static final DummyValueData INSTANCE = new DummyValueData();

    private DummyValueData() {
    }

    @Override
    public RandomDataInput bytes() {
        return ZeroBytesStore.INSTANCE;
    }

    @Override
    public long offset() {
        return 0;
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public DummyValue get() {
        return DummyValue.DUMMY_VALUE;
    }

    @Override
    public DummyValue getUsing(@Nullable DummyValue using) {
        return DummyValue.DUMMY_VALUE;
    }
}
