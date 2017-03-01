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

public final class CharSequenceUtf8DataAccess
        extends AbstractCharSequenceUtf8DataAccess<CharSequence> {

    public CharSequenceUtf8DataAccess() {
        this(DefaultElasticBytes.DEFAULT_BYTES_CAPACITY);
    }

    private CharSequenceUtf8DataAccess(long bytesCapacity) {
        super(bytesCapacity);
    }

    @Override
    public CharSequence getUsing(@Nullable CharSequence using) {
        StringBuilder sb;
        if (using instanceof StringBuilder) {
            sb = (StringBuilder) using;
            sb.setLength(0);
        } else {
            sb = new StringBuilder(cs.length());
        }
        sb.append(cs);
        return sb;
    }

    @Override
    public DataAccess<CharSequence> copy() {
        return new CharSequenceUtf8DataAccess(bytes().realCapacity());
    }
}
