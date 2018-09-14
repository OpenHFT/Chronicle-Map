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
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.StatefulCopyable;
import net.openhft.chronicle.values.Copyable;
import net.openhft.chronicle.values.Values;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ValueReader<T>
        implements SizedReader<T>, BytesReader<T>, StatefulCopyable<ValueReader<T>> {

    /**
     * Config field
     */
    private Class<T> valueType;

    // Cache fields
    private transient Class<? extends T> nativeClass;
    private transient Class<? extends T> heapClass;
    private transient Byteable nativeReference;

    public ValueReader(Class<T> valueType) {
        this.valueType = valueType;
        initTransients();
    }

    /**
     * Returns the interface of values deserialized.
     */
    protected Class<T> valueType() {
        return valueType;
    }

    protected Class<? extends T> nativeClass() {
        return nativeClass;
    }

    protected Class<? extends T> heapClass() {
        return heapClass;
    }

    private void initTransients() {
        nativeClass = Values.nativeClassFor(valueType);
        heapClass = Values.heapClassFor(valueType);
        nativeReference = (Byteable) Values.newNativeReference(valueType);
    }

    protected T createInstance() {
        try {
            return heapClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @NotNull
    @Override
    public T read(@NotNull Bytes in, long size, @Nullable T using) {
        if (size != nativeReference.maxSize())
            throw new IllegalArgumentException();
        return read(in, using);
    }

    @NotNull
    @Override
    public T read(Bytes in, @Nullable T using) {
        if (using != null && using.getClass() == nativeClass) {
            ((Byteable) using).bytesStore(in.bytesStore(), in.readPosition(),
                    nativeReference.maxSize());
            return using;
        }
        if (using == null)
            using = createInstance();
        nativeReference.bytesStore(in.bytesStore(), in.readPosition(),
                nativeReference.maxSize());
        ((Copyable) using).copyFrom(nativeReference);
        return using;
    }

    @Override
    public ValueReader<T> copy() {
        return new ValueReader<>(valueType);
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) {
        valueType = wireIn.read(() -> "valueType").typeLiteral();
        initTransients();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        wireOut.write(() -> "valueType").typeLiteral(valueType);
    }
}
