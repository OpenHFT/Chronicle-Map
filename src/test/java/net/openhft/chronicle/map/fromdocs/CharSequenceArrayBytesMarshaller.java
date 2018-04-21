/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
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

package net.openhft.chronicle.map.fromdocs;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.util.ReadResolvable;
import net.openhft.chronicle.hash.impl.util.Objects;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

public final class CharSequenceArrayBytesMarshaller
        implements BytesWriter<CharSequence[]>, BytesReader<CharSequence[]>,
        ReadResolvable<CharSequenceArrayBytesMarshaller> {

    static final CharSequenceArrayBytesMarshaller INSTANCE = new CharSequenceArrayBytesMarshaller();

    private CharSequenceArrayBytesMarshaller() {
    }

    @Override
    public void write(Bytes out, @NotNull CharSequence[] toWrite) {
        // Note that the *array length*, not the *serialization size* is written in the beginning,
        // so BytesWriter + BytesReader is the most suitable pair of serialization interfaces
        // to implement for CharSequence[] type
        out.writeInt(toWrite.length);
        for (CharSequence cs : toWrite) {
            // Assume elements non-null for simplicity
            Objects.requireNonNull(cs);
            out.writeUtf8(cs);
        }
    }

    @NotNull
    @Override
    public CharSequence[] read(Bytes in, @Nullable CharSequence[] using) {
        int len = in.readInt();
        if (using == null)
            using = new CharSequence[len];
        if (using.length != len)
            using = Arrays.copyOf(using, len);
        for (int i = 0; i < len; i++) {
            CharSequence cs = using[i];
            if (cs instanceof StringBuilder) {
                in.readUtf8((StringBuilder) cs);
            } else {
                StringBuilder sb = new StringBuilder(0);
                in.readUtf8(sb);
                using[i] = sb;
            }
        }
        return using;
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        // no fields to write
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) {
        // no fields to read
    }

    @Override
    public CharSequenceArrayBytesMarshaller readResolve() {
        return INSTANCE;
    }
}
