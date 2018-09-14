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
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.SizedReader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class BytesMarshallableReader<T extends BytesMarshallable>
        extends InstanceCreatingMarshaller<T> implements SizedReader<T>, BytesReader<T> {

    public BytesMarshallableReader(Class<T> tClass) {
        super(tClass);
    }

    @NotNull
    @Override
    public T read(@NotNull Bytes in, long size, @Nullable T using) {
        return read(in, using);
    }

    @NotNull
    @Override
    public T read(Bytes in, @Nullable T using) {
        if (using == null)
            using = createInstance();
        using.readMarshallable(in);
        return using;
    }
}
