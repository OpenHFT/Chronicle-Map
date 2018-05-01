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
import net.openhft.chronicle.hash.serialization.SizedReader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class StringBuilderSizedReader
        implements SizedReader<StringBuilder>, EnumMarshallable<StringBuilderSizedReader> {
    public static final StringBuilderSizedReader INSTANCE = new StringBuilderSizedReader();

    private StringBuilderSizedReader() {
    }

    @NotNull
    @Override
    public StringBuilder read(Bytes in, long size, @Nullable StringBuilder using) {
        if (0 > size || size > Integer.MAX_VALUE)
            throw new IllegalStateException("positive int size expected, " + size + " given");
        int csLen = (int) size;
        if (using == null) {
            using = new StringBuilder(csLen);
        } else {
            using.setLength(0);
            using.ensureCapacity(csLen);
        }
        BytesUtil.parseUtf8(in, using, csLen);
        return using;
    }

    @Override
    public StringBuilderSizedReader readResolve() {
        return INSTANCE;
    }
}
