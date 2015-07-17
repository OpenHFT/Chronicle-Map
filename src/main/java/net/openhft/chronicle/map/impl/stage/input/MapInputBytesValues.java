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

package net.openhft.chronicle.map.impl.stage.input;

import net.openhft.chronicle.hash.impl.stage.input.HashInputBytes;
import net.openhft.chronicle.map.impl.VanillaChronicleMapHolder;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public class MapInputBytesValues {
    
    @StageRef HashInputBytes in;
    @StageRef VanillaChronicleMapHolder<?, ?, ?, ?, ?, ?, ?> mh;
    
    @Stage("FirstInputValueOffsets") public long firstInputValueSize = -1;
    @Stage("FirstInputValueOffsets") public long firstInputValueOffset;
    
    private void initFirstInputValueOffsets() {
        in.inputBytes.position(in.inputKeyOffset + in.inputKeySize);
        firstInputValueSize = mh.m().valueSizeMarshaller.readSize(in.inputBytes);
        firstInputValueOffset = in.inputBytes.position();
    }

    @Stage("SecondInputValueOffsets") public long secondInputValueSize = -1;
    @Stage("SecondInputValueOffsets") public long secondInputValueOffset;

    private void initSecondInputValueOffsets() {
        in.inputBytes.position(firstInputValueOffset + firstInputValueSize);
        secondInputValueSize = mh.m().valueSizeMarshaller.readSize(in.inputBytes);
        secondInputValueOffset = in.inputBytes.position();
    }    
}
