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
import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.SizedWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class BooleanMarshaller
        implements SizedReader<Boolean>, BytesReader<Boolean>,
        SizedWriter<Boolean>, BytesWriter<Boolean>, EnumMarshallable<BooleanMarshaller> {
    public static final BooleanMarshaller INSTANCE = new BooleanMarshaller();

    private BooleanMarshaller() {
    }

    @Override
    public long size(@NotNull Boolean e) {
        return 1L;
    }

    @Override
    public void write(@NotNull Bytes out, long size, @NotNull Boolean toWrite) {
        write(out, toWrite);
    }

    @NotNull
    @Override
    public Boolean read(@NotNull Bytes in, long size, Boolean using) {
        return read(in, null);
    }

    @NotNull
    @Override
    public Boolean read(Bytes in, @Nullable Boolean using) {
        return in.readByte() != 0;
    }

    @Override
    public void write(Bytes out, @NotNull Boolean toWrite) {
        out.writeByte((byte) (toWrite ? 'Y' : 0));
    }

    @Override
    public BooleanMarshaller readResolve() {
        return INSTANCE;
    }
}
