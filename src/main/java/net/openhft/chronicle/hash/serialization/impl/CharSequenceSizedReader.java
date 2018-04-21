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

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.core.util.ReadResolvable;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.StatefulCopyable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class CharSequenceSizedReader implements SizedReader<CharSequence>,
        StatefulCopyable<CharSequenceSizedReader>, ReadResolvable<CharSequenceSizedReader> {

    public static final CharSequenceSizedReader INSTANCE = new CharSequenceSizedReader();

    /**
     * @deprecated use {@link #INSTANCE} as {@code CharSequenceSizedReader} is immutable and
     * stateless
     */
    @Deprecated
    public CharSequenceSizedReader() {
    }

    @NotNull
    @Override
    public CharSequence read(
            @NotNull Bytes in, long size, @Nullable CharSequence using) {
        if (0 > size || size > Integer.MAX_VALUE)
            throw new IllegalStateException("positive int size expected, " + size + " given");
        int csLen = (int) size;
        StringBuilder usingSB;
        if (using instanceof StringBuilder) {
            usingSB = ((StringBuilder) using);
            usingSB.setLength(0);
            usingSB.ensureCapacity(csLen);
        } else {
            usingSB = new StringBuilder(csLen);
        }
        BytesUtil.parseUtf8(in, usingSB, csLen);
        return usingSB;
    }

    @Override
    public CharSequenceSizedReader copy() {
        return INSTANCE;
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) {
        // no fields to read
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        // no fields to write
    }

    @Override
    public CharSequenceSizedReader readResolve() {
        return INSTANCE;
    }
}
