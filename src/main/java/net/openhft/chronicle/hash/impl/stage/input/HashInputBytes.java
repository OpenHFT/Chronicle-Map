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
