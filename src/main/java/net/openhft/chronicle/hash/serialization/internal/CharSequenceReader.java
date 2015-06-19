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

import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.lang.io.AbstractBytes;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.pool.CharSequenceInterner;
import net.openhft.lang.threadlocal.StatefulCopyable;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.io.UncheckedIOException;

public final class CharSequenceReader<S extends CharSequence>
        implements BytesReader<S>, StatefulCopyable<CharSequenceReader<S>> {
    private static final long serialVersionUID = 0L;

    private enum NoInterningIdentity implements CharSequenceInterner<CharSequence> {
        INSTANCE;

        @Override
        public CharSequence intern(CharSequence cs) {
            return cs;
        }
    }

    private enum NoInterningToString implements CharSequenceInterner<String> {
        INSTANCE;
        @Override
        public String intern(CharSequence cs) {
            return cs.toString();
        }
    }

    private static final CharSequenceReader<String> STRING_READER =
            new CharSequenceReader<String>(NoInterningToString.INSTANCE, NoInterningToString.INSTANCE);

    private static final CharSequenceReader DEFAULT_READER =
            new CharSequenceReader(NoInterningIdentity.INSTANCE, NoInterningIdentity.INSTANCE);

    public static CharSequenceReader of() {
        return STRING_READER;
    }

    public static CharSequenceReader<String> ofStringBuilder() {
        return DEFAULT_READER;
    }

    public static <S extends CharSequence, I extends CharSequenceInterner<S> & Serializable>
    CharSequenceReader<S> of(@NotNull I interner, @NotNull Serializable stateIdentity) {
        return new CharSequenceReader<S>(interner, stateIdentity);
    }

    private final StringBuilder sb = new StringBuilder(128);
    @NotNull
    private final CharSequenceInterner<S> interner;
    @NotNull
    private final Serializable identity;

    private CharSequenceReader(@NotNull CharSequenceInterner<S> interner,
                               @NotNull Serializable identity) {
        this.interner = interner;
        this.identity = identity;
    }

    @Override
    public S read(Bytes bytes, long size) {
        sb.setLength(0);
        try {
            ((AbstractBytes) bytes).readUTF0(sb, (int) size);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return interner.intern(sb);
    }

    @Override
    public S read(Bytes bytes, long size, S toReuse) {
        Appendable appendable;
        if (toReuse instanceof Appendable) {
            appendable = (Appendable) toReuse;
            if (toReuse instanceof StringBuilder) {
                ((StringBuilder) toReuse).setLength(0);
            } else if (toReuse instanceof StringBuffer) {
                ((StringBuffer) toReuse).setLength(0);
            }
        } else {
            sb.setLength(0);
            appendable = sb;
        }
        try {
            ((AbstractBytes) bytes).readUTF0(appendable, (int) size);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (appendable == toReuse)
            return toReuse;
        return interner.intern(sb);
    }

    @Override
    public Object stateIdentity() {
        return identity;
    }

    @Override
    public CharSequenceReader<S> copy() {
        return new CharSequenceReader<S>(interner, identity);
    }

    private Object readResolve() throws ObjectStreamException {
        if (interner == NoInterningToString.INSTANCE)
            return STRING_READER;
        if (interner == NoInterningIdentity.INSTANCE)
            return DEFAULT_READER;
        return this;
    }
}
