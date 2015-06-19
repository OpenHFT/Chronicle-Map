/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.hash.serialization.internal;

import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.lang.io.AbstractBytes;
import net.openhft.lang.io.Bytes;

import java.io.ObjectStreamException;

public final class CharSequenceWriter<CS extends CharSequence> implements BytesWriter<CS> {
    private static final long serialVersionUID = 0L;
    private static final CharSequenceWriter INSTANCE = new CharSequenceWriter();

    public static <CS extends CharSequence> CharSequenceWriter<CS> instance() {
        return INSTANCE;
    }

    private CharSequenceWriter() {}

    @Override
    public long size(CS s) {
        return AbstractBytes.findUTFLength(s, s.length());
    }

    @Override
    public void write(Bytes bytes, CS s) {
        AbstractBytes.writeUTF0(bytes, s, s.length());
    }

    private Object readResolve() throws ObjectStreamException {
        return INSTANCE;
    }
}
