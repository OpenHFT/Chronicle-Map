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

package net.openhft.chronicle.hash.impl.stage.input;

import net.openhft.chronicle.hash.impl.JavaLangBytesReusableBytesStore;
import net.openhft.chronicle.hash.impl.VanillaChronicleHashHolder;
import net.openhft.lang.io.Bytes;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public class HashInputBytes {
    
    @StageRef VanillaChronicleHashHolder<?, ?, ?> hh;
    
    public Bytes inputBytes = null;
    public final JavaLangBytesReusableBytesStore inputStore = new JavaLangBytesReusableBytesStore();
    
    public void initInputBytes(Bytes inputBytes) {
        this.inputBytes = inputBytes;
        inputStore.setBytes(inputBytes);
    }
    
    @Stage("InputKeyOffsets") public long inputKeySize = -1;
    @Stage("InputKeyOffsets") public long inputKeyOffset;
    
    private void initInputKeyOffsets() {
        inputKeySize = hh.h().keySizeMarshaller.readSize(inputBytes);
        inputKeyOffset = inputBytes.position();
    }
}
