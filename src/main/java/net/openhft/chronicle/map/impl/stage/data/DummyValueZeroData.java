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

package net.openhft.chronicle.map.impl.stage.data;

import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.bytes.ZeroRandomDataInput;
import net.openhft.chronicle.hash.AbstractData;
import net.openhft.chronicle.map.impl.VanillaChronicleMapHolder;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public class DummyValueZeroData<V> extends AbstractData<V> {
    @StageRef
    VanillaChronicleMapHolder<?, ?, ?, ?, ?, ?, ?> mh;

    @Override
    public RandomDataInput bytes() {
        return ZeroRandomDataInput.INSTANCE;
    }

    @Override
    public long offset() {
        return 0;
    }

    @Override
    public long size() {
        return mh.m().valueSizeMarshaller.minEncodableSize();
    }

    @Override
    public V get() {
        throw new UnsupportedOperationException();
    }

    @Override
    public V getUsing(V usingInstance) {
        throw new UnsupportedOperationException();
    }
}
