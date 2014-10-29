/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.lang.io.AbstractBytes;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.pool.CharSequenceInterner;
import net.openhft.lang.threadlocal.StatefulCopyable;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;

public final class CharSequenceReader<S extends CharSequence>
        implements BytesReader<S>, StatefulCopyable<CharSequenceReader<S>> {
    private static final long serialVersionUID = 0L;

    private enum NoInterning implements CharSequenceInterner<String> {
        INSTANCE;
        @Override
        public String intern(CharSequence cs) {
            return cs.toString();
        }
    }

    private static final CharSequenceReader<String> DEFAULT_READER =
            new CharSequenceReader<String>(NoInterning.INSTANCE, NoInterning.INSTANCE);


    public static CharSequenceReader<String> of() {
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
            AbstractBytes.readUTF0(bytes, sb, (int) size);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return interner.intern(sb);
    }

    @Override
    public S read(Bytes bytes, long size, S s) {
        Appendable appendable;
        if (s instanceof Appendable) {
            appendable = (Appendable) s;
            if (s instanceof StringBuilder) {
                ((StringBuilder) s).setLength(0);
            } else if (s instanceof StringBuffer) {
                ((StringBuffer) s).setLength(0);
            }
        } else {
            sb.setLength(0);
            appendable = sb;
        }
        try {
            AbstractBytes.readUTF0(bytes, appendable, (int) size);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        if (appendable == s)
            return s;
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
        if (interner == NoInterning.INSTANCE)
            return DEFAULT_READER;
        return this;
    }
}
