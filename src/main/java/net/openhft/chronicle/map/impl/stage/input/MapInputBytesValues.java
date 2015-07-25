/*
 *      Copyright (C) 2015  higherfrequencytrading.com
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
