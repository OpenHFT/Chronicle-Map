/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.ListMarshaller;
import net.openhft.chronicle.hash.serialization.SetMarshaller;
import net.openhft.chronicle.hash.serialization.StatefulCopyable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link BytesReader} implementation for {@code CharSequence}. For the primary ChronicleMap's key
 * or value type {@link CharSequenceSizedReader} + {@link CharSequenceUtf8DataAccess} are more
 * efficient (because don't store the size twice), so this reader is useful in conjunction with
 * {@link ListMarshaller} or {@link SetMarshaller}.
 *
 * @see CharSequenceBytesWriter
 */
public final class CharSequenceBytesReader implements BytesReader<CharSequence>,
        StatefulCopyable<CharSequenceBytesReader>, EnumMarshallable<CharSequenceBytesReader> {
    public static final CharSequenceBytesReader INSTANCE = new CharSequenceBytesReader();

    private CharSequenceBytesReader() {
    }

    @NotNull
    @Override
    public CharSequence read(Bytes in, @Nullable CharSequence using) {
        StringBuilder usingSB;
        if (using instanceof StringBuilder) {
            usingSB = (StringBuilder) using;
        } else {
            usingSB = new StringBuilder();
        }
        if (in.readUtf8(usingSB)) {
            return usingSB;
        } else {
            throw new NullPointerException("BytesReader couldn't read null");
        }
    }

    @Override
    public CharSequenceBytesReader copy() {
        return INSTANCE;
    }

    @Override
    public CharSequenceBytesReader readResolve() {
        return INSTANCE;
    }
}
