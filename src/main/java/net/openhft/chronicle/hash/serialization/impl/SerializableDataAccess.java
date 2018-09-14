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
import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.hash.AbstractData;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;

import static net.openhft.chronicle.hash.serialization.impl.DefaultElasticBytes.DEFAULT_BYTES_CAPACITY;

public class SerializableDataAccess<T extends Serializable> extends AbstractData<T>
        implements DataAccess<T> {

    // Cache fields
    transient Bytes bytes;
    transient OutputStream out;
    transient InputStream in;

    /**
     * State field
     */
    transient T instance;

    public SerializableDataAccess() {
        this(DEFAULT_BYTES_CAPACITY);
    }

    SerializableDataAccess(long bytesCapacity) {
        initTransients(bytesCapacity);
    }

    void initTransients(long bytesCapacity) {
        bytes = DefaultElasticBytes.allocateDefaultElasticBytes(bytesCapacity);
        out = bytes.outputStream();
        in = bytes.inputStream();
    }

    @Override
    public RandomDataInput bytes() {
        return bytes.bytesStore();
    }

    @Override
    public long offset() {
        return 0;
    }

    @Override
    public long size() {
        return bytes.readRemaining();
    }

    @Override
    public T get() {
        return instance;
    }

    // TODO reuse using object
    @Override
    public T getUsing(@Nullable T using) {
        try {
            T result = (T) new ObjectInputStream(in).readObject();
            bytes.readPosition(0);
            return result;
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Data<T> getData(@NotNull T instance) {
        this.instance = instance;
        bytes.clear();
        try {
            ObjectOutputStream out = new ObjectOutputStream(this.out);
            out.writeObject(instance);
            out.flush();
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void uninit() {
        instance = null;
    }

    @Override
    public DataAccess<T> copy() {
        return new SerializableDataAccess<>(bytes.realCapacity());
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) {
        // no fields to read
        initTransients(DEFAULT_BYTES_CAPACITY);
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        // no fields to write
    }
}
