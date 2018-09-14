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

import net.openhft.chronicle.bytes.Byteable;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.serialization.SizedReader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Reader of {@link Byteable} subclasses.
 *
 * @param <T> the subtype of {@link Byteable} deserialized
 */
public class ByteableSizedReader<T extends Byteable> extends InstanceCreatingMarshaller<T>
        implements SizedReader<T> {

    public ByteableSizedReader(Class<T> tClass) {
        super(tClass);
    }

    @NotNull
    @Override
    public final T read(@NotNull Bytes in, long size, @Nullable T using) {
        if (using == null)
            using = createInstance();
        using.bytesStore(in.bytesStore(), in.readPosition(), size);
        return using;
    }
}
