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
 * As a writer for top-level Chronicle Map's key or value type (Long), {@code LongMarshaller} is
 * deprecated in favor of {@link LongDataAccess}. As reader and element writer for {@link
 * ListMarshaller} and similar composite marshallers, {@code LongMarshaller} is not deprecated.
 */
public final class LongMarshaller
        implements SizedReader<Long>, BytesReader<Long>,
        SizedWriter<Long>, BytesWriter<Long>, EnumMarshallable<LongMarshaller> {
    public static final LongMarshaller INSTANCE = new LongMarshaller();

    private LongMarshaller() {
    }

    @Override
    public long size(@NotNull Long toWrite) {
        return 8L;
    }

    @NotNull
    @Override
    public Long read(@NotNull Bytes in, long size, @Nullable Long using) {
        return in.readLong();
    }

    @Override
    public void write(@NotNull Bytes out, long size, @NotNull Long toWrite) {
        out.writeLong(toWrite);
    }

    @NotNull
    @Override
    public Long read(Bytes in, @Nullable Long using) {
        return in.readLong();
    }

    @Override
    public void write(Bytes out, @NotNull Long toWrite) {
        out.writeLong(toWrite);
    }

    @Override
    public LongMarshaller readResolve() {
        return INSTANCE;
    }
}
