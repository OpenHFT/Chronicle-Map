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
import net.openhft.chronicle.hash.serialization.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * As a writer for top-level Chronicle Map's key or value type (Integer), {@code IntegerMarshaller}
 * is deprecated in favor of {@link IntegerDataAccess_3_13}. As reader and element writer for {@link
 * ListMarshaller} and similar composite marshallers, {@code IntegerMarshaller} is not deprecated.
 */
public final class IntegerMarshaller
        implements SizedReader<Integer>, BytesReader<Integer>,
        SizedWriter<Integer>, BytesWriter<Integer>, EnumMarshallable<IntegerMarshaller> {
    public static final IntegerMarshaller INSTANCE = new IntegerMarshaller();

    private IntegerMarshaller() {
    }

    @NotNull
    @Override
    public Integer read(@NotNull Bytes in, long size, @Nullable Integer using) {
        return in.readInt();
    }

    @Override
    public long size(@NotNull Integer toWrite) {
        return 4L;
    }

    @Override
    public void write(@NotNull Bytes out, long size, @NotNull Integer toWrite) {
        out.writeInt(toWrite);
    }

    @NotNull
    @Override
    public Integer read(Bytes in, @Nullable Integer using) {
        return in.readInt();
    }

    @Override
    public void write(Bytes out, @NotNull Integer toWrite) {
        out.writeInt(toWrite);
    }

    @Override
    public IntegerMarshaller readResolve() {
        return INSTANCE;
    }
}
