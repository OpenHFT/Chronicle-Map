/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.SizedWriter;

import java.lang.reflect.Type;

import static net.openhft.chronicle.hash.serialization.StatefulCopyable.copyIfNeeded;
import static net.openhft.chronicle.hash.serialization.impl.DefaultElasticBytes.DEFAULT_BYTES_CAPACITY;

/**
 * {@link SizedMarshallableDataAccess} variant for writers that do not reuse instances.
 *
 * <p>This implementation always returns {@code null} from {@link #createInstance()}, forcing
 * callers to supply their own instance when reading. It is useful when the writer logic
 * itself manages object reuse or when values are treated as immutable snapshots.
 */
public class NotReusingSizedMarshallableDataAccess<T> extends SizedMarshallableDataAccess<T> {

    public NotReusingSizedMarshallableDataAccess(
            Type tClass, SizedReader<T> sizedReader, SizedWriter<? super T> sizedWriter) {
        super(tClass, sizedReader, sizedWriter, DEFAULT_BYTES_CAPACITY);
    }

    @Override
    protected T createInstance() {
        return null;
    }

    @Override
    public DataAccess<T> copy() {
        return new NotReusingSizedMarshallableDataAccess<>(
                tType(), copyIfNeeded(sizedReader()), copyIfNeeded(sizedWriter()));
    }
}
