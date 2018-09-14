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
import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.chronicle.hash.serialization.ListMarshaller;
import net.openhft.chronicle.hash.serialization.SetMarshaller;
import org.jetbrains.annotations.NotNull;

/**
 * {@link BytesWriter} implementation for {@link CharSequence}, for the primary ChronicleMap's key
 * or value type {@link CharSequenceUtf8DataAccess} + {@link CharSequenceSizedReader} are more
 * efficient (because don't store the size twice), so this writer is useful in conjunction with
 * {@link ListMarshaller} or {@link SetMarshaller}.
 *
 * @see StringBytesReader
 */
public final class CharSequenceBytesWriter
        implements BytesWriter<CharSequence>, EnumMarshallable<CharSequenceBytesWriter> {
    public static final CharSequenceBytesWriter INSTANCE = new CharSequenceBytesWriter();

    private CharSequenceBytesWriter() {
    }

    @Override
    public void write(Bytes out, @NotNull CharSequence toWrite) {
        if (toWrite == null)
            throw new NullPointerException("BytesWriter couldn't write null");
        out.writeUtf8(toWrite);
    }

    @Override
    public CharSequenceBytesWriter readResolve() {
        return INSTANCE;
    }
}
