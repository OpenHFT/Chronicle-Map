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

import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.SizedWriter;

import java.lang.reflect.Type;

import static net.openhft.chronicle.hash.serialization.StatefulCopyable.copyIfNeeded;
import static net.openhft.chronicle.hash.serialization.impl.DefaultElasticBytes.DEFAULT_BYTES_CAPACITY;

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
