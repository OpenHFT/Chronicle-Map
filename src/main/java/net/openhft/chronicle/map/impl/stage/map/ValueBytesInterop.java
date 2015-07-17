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

package net.openhft.chronicle.map.impl.stage.map;

import net.openhft.chronicle.hash.impl.stage.hash.ThreadLocalCopiesHolder;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.internal.MetaBytesInterop;
import net.openhft.chronicle.map.impl.VanillaChronicleMapHolder;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public class ValueBytesInterop<V, VI, MVI extends MetaBytesInterop<V, ? super VI>> {
    @StageRef VanillaChronicleMapHolder<?, ?, ?, V, VI, MVI, ?> mh;
    @StageRef ThreadLocalCopiesHolder ch;
    
    public final BytesReader<V> valueReader =
            mh.m().valueReaderProvider.get(ch.copies, mh.m().originalValueReader);
    public final VI valueInterop =
            mh.m().valueInteropProvider.get(ch.copies, mh.m().originalValueInterop);
    
    public MVI valueMetaInterop(V value) {
        return mh.m().metaValueInteropProvider.get(
                ch.copies, mh.m().originalMetaValueInterop, valueInterop, value);
    }
}
