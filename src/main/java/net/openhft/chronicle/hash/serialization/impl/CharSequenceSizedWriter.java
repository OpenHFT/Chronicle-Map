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
import net.openhft.chronicle.hash.serialization.SizedWriter;
import org.jetbrains.annotations.NotNull;

public enum CharSequenceSizedWriter implements SizedWriter<CharSequence> {
    INSTANCE;

    @Override
    public long size(@NotNull CharSequence toWrite) {
        return BytesUtil.utf8Length(toWrite);
    }

    @Override
    public void write(@NotNull Bytes out, long size, @NotNull CharSequence toWrite) {
        BytesUtil.appendUtf8(out, toWrite);
    }
}
