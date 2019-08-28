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

import net.openhft.chronicle.bytes.*;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.wire.WireIn;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Type;

import static net.openhft.chronicle.hash.serialization.impl.DefaultElasticBytes.DEFAULT_BYTES_CAPACITY;

public class BytesMarshallableDataAccess<T extends BytesMarshallable>
        extends InstanceCreatingMarshaller<T> implements DataAccess<T>, Data<T> {

    // Cache fields
    private transient boolean bytesInit;
    private transient Bytes bytes;
    private transient VanillaBytes targetBytes;

    /**
     * State field
     */
    private transient T instance;

    public BytesMarshallableDataAccess(Class<T> tClass) {
        this(tClass, DEFAULT_BYTES_CAPACITY);
    }

    private BytesMarshallableDataAccess(Type tClass, long bytesCapacity) {
        super(tClass);
        initTransients(bytesCapacity);
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) {
        super.readMarshallable(wireIn);
        initTransients(DEFAULT_BYTES_CAPACITY);
    }

    private void initTransients(long bytesCapacity) {
        bytes = DefaultElasticBytes.allocateDefaultElasticBytes(bytesCapacity);
        targetBytes = VanillaBytes.vanillaBytes();
    }

    @Override
    public RandomDataInput bytes() {
        initBytes();
        return bytes.bytesStore();
    }

    private void initBytes() {
        if (!bytesInit) {
            bytes.clear();
            instance.writeMarshallable(bytes);
            bytesInit = true;
        }
    }

    @Override
    public long offset() {
        return 0;
    }

    @Override
    public long size() {
        initBytes();
        return bytes.readRemaining();
    }

    @Override
    public void writeTo(RandomDataOutput target, long targetOffset) {
        if (bytesInit) {
            target.write(targetOffset, bytes(), offset(), size());
        } else {
            targetBytes.bytesStore((BytesStore) target, targetOffset,
                    target.capacity() - targetOffset);
            targetBytes.writePosition(targetOffset);
            instance.writeMarshallable(targetBytes);
            targetBytes.bytesStore(NoBytesStore.NO_BYTES_STORE, 0, 0);
        }
    }

    @Override
    public T get() {
        return instance;
    }

    @Override
    public T getUsing(@Nullable T using) {
        if (using == null)
            using = createInstance();
        initBytes();
        using.readMarshallable(bytes);
        bytes.readPosition(0);
        return using;
    }

    @Override
    public int hashCode() {
        return dataHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return dataEquals(obj);
    }

    @Override
    public String toString() {
        return get().toString();
    }

    @Override
    public Data<T> getData(@NotNull T instance) {
        this.instance = instance;
        bytesInit = false;
        return this;
    }

    @Override
    public void uninit() {
        instance = null;
    }

    @Override
    public DataAccess<T> copy() {
        return new BytesMarshallableDataAccess<>(tType(), bytes.realCapacity());
    }
}
