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
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.SizedWriter;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static net.openhft.chronicle.hash.serialization.StatefulCopyable.copyIfNeeded;
import static net.openhft.chronicle.hash.serialization.impl.DefaultElasticBytes.DEFAULT_BYTES_CAPACITY;

public class SizedMarshallableDataAccess<T> extends InstanceCreatingMarshaller<T>
        implements DataAccess<T>, Data<T> {

    // Config fields
    private SizedReader<T> sizedReader;
    private SizedWriter<? super T> sizedWriter;

    // Cache fields
    private transient boolean bytesInit;
    private transient Bytes bytes;
    private transient long size;
    private transient VanillaBytes targetBytes;

    /**
     * State field
     */
    private transient T instance;

    public SizedMarshallableDataAccess(
            Class<T> tClass, SizedReader<T> sizedReader, SizedWriter<? super T> sizedWriter) {
        this(tClass, sizedReader, sizedWriter, DEFAULT_BYTES_CAPACITY);
    }

    private SizedMarshallableDataAccess(
            Class<T> tClass, SizedReader<T> sizedReader, SizedWriter<? super T> sizedWriter,
            long bytesCapacity) {
        super(tClass);
        this.sizedWriter = sizedWriter;
        this.sizedReader = sizedReader;
        initTransients(bytesCapacity);
    }

    SizedReader<T> sizedReader() {
        return sizedReader;
    }

    SizedWriter<? super T> sizedWriter() {
        return sizedWriter;
    }

    private void initTransients(long bytesCapacity) {
        bytes = DefaultElasticBytes.allocateDefaultElasticBytes(bytesCapacity);
        targetBytes = VanillaBytes.vanillaBytes();
    }

    @Override
    public RandomDataInput bytes() {
        if (!bytesInit) {
            bytes.clear();
            sizedWriter.write(bytes, size, instance);
            bytesInit = true;
        }
        return bytes.bytesStore();
    }

    @Override
    public long offset() {
        return 0;
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public void writeTo(RandomDataOutput target, long targetOffset) {
        if (bytesInit || !(target instanceof BytesStore)) {
            target.write(targetOffset, bytes(), offset(), size);
        } else {
            targetBytes.bytesStore((BytesStore) target, targetOffset, size);
            targetBytes.writePosition(targetOffset);
            sizedWriter.write(targetBytes, size, instance);
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
        T result = sizedReader.read(bytes, size(), using);
        bytes.readPosition(0);
        return result;
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
        this.size = sizedWriter.size(instance);
        bytesInit = false;
        return this;
    }

    @Override
    public void uninit() {
        instance = null;
    }

    @Override
    public DataAccess<T> copy() {
        return new SizedMarshallableDataAccess<>(
                tClass(), copyIfNeeded(sizedReader), copyIfNeeded(sizedWriter),
                bytes.realCapacity());
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) {
        super.readMarshallable(wireIn);
        sizedReader = wireIn.read(() -> "sizedReader").typedMarshallable();
        sizedWriter = wireIn.read(() -> "sizedWriter").typedMarshallable();
        initTransients(DEFAULT_BYTES_CAPACITY);
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        super.writeMarshallable(wireOut);
        wireOut.write(() -> "sizedReader").typedMarshallable(sizedReader);
        wireOut.write(() -> "sizedWriter").typedMarshallable(sizedWriter);
    }
}
