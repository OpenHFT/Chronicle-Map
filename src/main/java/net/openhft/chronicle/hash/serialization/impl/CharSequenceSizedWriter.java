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
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.hash.serialization.SizedWriter;
import org.jetbrains.annotations.NotNull;

/**
 * @deprecated use one of {@link AbstractCharSequenceUtf8DataAccess} subclasses instead. This class
 * is not removed, because users who created a chronicleMap with older version of the library with
 * this class as the key or value writer should be able to read and access the map with the present
 * version of the library.
 */
@Deprecated
@SuppressWarnings("deprecation")
public final class CharSequenceSizedWriter
        implements SizedWriter<CharSequence>, EnumMarshallable<CharSequenceSizedWriter> {
    public static final CharSequenceSizedWriter INSTANCE = new CharSequenceSizedWriter();

    private CharSequenceSizedWriter() {
    }

    @Override
    public long size(@NotNull CharSequence toWrite) {
        return BytesUtil.utf8Length(toWrite);
    }

    @Override
    public void write(@NotNull Bytes out, long size, @NotNull CharSequence toWrite) {
        BytesUtil.appendUtf8(out, toWrite);
    }

    @Override
    public CharSequenceSizedWriter readResolve() {
        return INSTANCE;
    }
}
