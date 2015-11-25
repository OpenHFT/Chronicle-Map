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

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.bytes.IORuntimeException;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.StatefulCopyable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;

public class CharSequenceSizedReader
        implements SizedReader<CharSequence>, StatefulCopyable<CharSequenceSizedReader> {

    /** Cache field */
    private transient StringBuilder sb;

    public CharSequenceSizedReader() {
        initTransients();
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        initTransients();
    }

    private void initTransients() {
        sb = new StringBuilder();
    }

    @NotNull
    @Override
    public CharSequence read(
            @NotNull Bytes in, long size, @Nullable CharSequence using) {
        if (0 > size || size > Integer.MAX_VALUE)
            throw new IllegalStateException("positive int size expected, " + size + " given");
        int csLen = (int) size;
        if (!(using instanceof StringBuilder)) {
            sb.setLength(0);
            sb.ensureCapacity(csLen);
            BytesUtil.parseUtf8(in, sb, csLen);
            return sb.toString();
        } else {
            StringBuilder usingSB = (StringBuilder) using;
            usingSB.setLength(0);
            usingSB.ensureCapacity(csLen);
            BytesUtil.parseUtf8(in, usingSB, csLen);
            return usingSB;
        }
    }

    @Override
    public CharSequenceSizedReader copy() {
        return new CharSequenceSizedReader();
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) throws IORuntimeException {
        // no fields to read
        initTransients();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        // no fields to write
    }
}
