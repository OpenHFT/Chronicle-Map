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

import net.openhft.chronicle.hash.serialization.DataAccess;
import org.jetbrains.annotations.Nullable;

import static net.openhft.chronicle.hash.serialization.impl.DefaultElasticBytes.DEFAULT_BYTES_CAPACITY;

public final class StringBuilderUtf8DataAccess
        extends AbstractCharSequenceUtf8DataAccess<StringBuilder> {

    public StringBuilderUtf8DataAccess() {
        this(DEFAULT_BYTES_CAPACITY);
    }

    private StringBuilderUtf8DataAccess(long bytesCapacity) {
        super(bytesCapacity);
    }

    @Override
    public StringBuilder getUsing(@Nullable StringBuilder using) {
        if (using != null) {
            using.setLength(0);
        } else {
            using = new StringBuilder(cs.length());
        }
        using.append(cs);
        return using;
    }

    @Override
    public DataAccess<StringBuilder> copy() {
        return new StringBuilderUtf8DataAccess(bytes().realCapacity());
    }
}
