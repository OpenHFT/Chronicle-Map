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
 * As a writer for top-level Chronicle Map's key or value type (Long), {@code DoubleMarshaller} is
 * deprecated in favor of {@link DoubleDataAccess}. As reader and element writer for {@link
 * ListMarshaller} and similar composite marshallers, {@code DoubleMarshaller} is not deprecated.
 */
public final class DoubleMarshaller
        implements SizedReader<Double>, BytesReader<Double>,
        SizedWriter<Double>, BytesWriter<Double>, EnumMarshallable<DoubleMarshaller> {
    public static final DoubleMarshaller INSTANCE = new DoubleMarshaller();

    private DoubleMarshaller() {
    }

    @NotNull
    @Override
    public Double read(@NotNull Bytes in, long size, @Nullable Double using) {
        return in.readDouble();
    }

    @Override
    public long size(@NotNull Double toWrite) {
        return 8L;
    }

    @Override
    public void write(@NotNull Bytes out, long size, @NotNull Double toWrite) {
        out.writeDouble(toWrite);
    }

    @NotNull
    @Override
    public Double read(Bytes in, @Nullable Double using) {
        return in.readDouble();
    }

    @Override
    public void write(Bytes out, @NotNull Double toWrite) {
        out.writeDouble(toWrite);
    }

    @Override
    public DoubleMarshaller readResolve() {
        return INSTANCE;
    }
}
