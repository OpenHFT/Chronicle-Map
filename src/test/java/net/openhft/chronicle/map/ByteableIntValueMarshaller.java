/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.serialization.impl.ByteableMarshaller;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.NativeBytes;
import net.openhft.lang.io.serialization.BytesMarshaller;
import net.openhft.lang.io.serialization.ObjectFactory;
import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.values.IntValue;
import org.jetbrains.annotations.Nullable;

enum ByteableIntValueMarshaller implements BytesMarshaller<IntValue> {
    INSTANCE;

    static final Class DIRECT = DataValueClasses.directClassFor(IntValue.class);

    @Override
    public void write(Bytes bytes, IntValue intValue) {
        Byteable biv = (Byteable) intValue;
        bytes.write(biv.bytes(), biv.offset(), biv.maxSize());
    }

    @Nullable
    @Override
    public IntValue read(Bytes bytes) {
        return read(bytes, null);
    }

    @Nullable
    @Override
    public IntValue read(Bytes bytes, @Nullable IntValue intValue) {
        try {
            if (intValue == null)
                intValue = (IntValue) NativeBytes.UNSAFE.allocateInstance(DIRECT);
            Byteable biv = (Byteable) intValue;
            ByteableMarshaller.setBytesAndOffset(biv, bytes);
            bytes.skip(biv.maxSize());
            return intValue;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }

    }
}

enum DirectIntValueFactory implements ObjectFactory<IntValue> {
    INSTANCE;

    @Override
    public IntValue create() {
        try {
            return (IntValue) NativeBytes.UNSAFE.allocateInstance(ByteableIntValueMarshaller.DIRECT);
        } catch (InstantiationException e) {
            throw new AssertionError(e);
        }
    }
}
