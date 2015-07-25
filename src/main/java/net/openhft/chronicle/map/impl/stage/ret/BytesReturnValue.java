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

package net.openhft.chronicle.map.impl.stage.ret;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.JavaLangBytesReusableBytesStore;
import net.openhft.chronicle.map.ReturnValue;
import net.openhft.chronicle.map.TcpReplicator.TcpSocketChannelEntryWriter;
import net.openhft.chronicle.map.impl.VanillaChronicleMapHolder;
import net.openhft.lang.io.Bytes;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;

@Staged
public class BytesReturnValue<V> implements ReturnValue<V>, AutoCloseable {

    @StageRef
    VanillaChronicleMapHolder<?, ?, ?, V, ?, ?, ?> mh;

    private final JavaLangBytesReusableBytesStore outputStore =
            new JavaLangBytesReusableBytesStore();

    @Stage("Output") TcpSocketChannelEntryWriter output = null;
    @Stage("Output") long startOutputPos;

    public void initOutput(TcpSocketChannelEntryWriter output) {
        this.output = output;
        startOutputPos = output.in().position();
    }

    @Override
    public void returnValue(@NotNull Data<V> value) {
        long valueSize = value.size();
        long totalSize = 1L + mh.m().valueSizeMarshaller.sizeEncodingSize(valueSize) + valueSize;
        output.ensureBufferSize(totalSize);
        Bytes out = output.in();
        out.writeBoolean(false);
        mh.m().valueSizeMarshaller.writeSize(out, valueSize);
        long outPosition = out.position();
        out.skip(valueSize);
        outputStore.setBytes(out);
        value.writeTo(outputStore, outPosition);
    }

    @Override
    public void close() {
        if (output.in().position() == startOutputPos) {
            output.ensureBufferSize(1L);
            output.in().writeBoolean(true);
        }
    }
}
