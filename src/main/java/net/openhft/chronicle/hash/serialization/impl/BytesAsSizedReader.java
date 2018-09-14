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
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.StatefulCopyable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class BytesAsSizedReader<T>
        implements SizedReader<T>, StatefulCopyable<BytesAsSizedReader<T>> {

    /**
     * Config field
     */
    private BytesReader<T> reader;

    public BytesAsSizedReader(BytesReader<T> reader) {
        this.reader = reader;
    }

    @NotNull
    @Override
    public T read(Bytes in, long size, @Nullable T using) {
        return reader.read(in, using);
    }

    @Override
    public BytesAsSizedReader<T> copy() {
        if (reader instanceof StatefulCopyable) {
            return new BytesAsSizedReader<>(StatefulCopyable.copyIfNeeded(reader));
        } else {
            return this;
        }
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) {
        reader = wireIn.read(() -> "reader").typedMarshallable();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        wireOut.write(() -> "reader").typedMarshallable(reader);
    }
}
