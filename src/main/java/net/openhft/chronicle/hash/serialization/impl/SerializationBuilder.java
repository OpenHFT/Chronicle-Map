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
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.bytes.DynamicallySized;
import net.openhft.chronicle.core.OS;
import org.jetbrains.annotations.NotNull;
import net.openhft.chronicle.core.util.ReadResolvable;
import net.openhft.chronicle.hash.serialization.*;
import net.openhft.chronicle.values.ValueModel;
import net.openhft.chronicle.values.Values;
import net.openhft.chronicle.wire.Marshallable;

import java.io.Externalizable;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;

import static net.openhft.chronicle.hash.serialization.SizeMarshaller.constant;
import static net.openhft.chronicle.hash.serialization.SizeMarshaller.stopBit;

public final class SerializationBuilder<T> implements Cloneable {

    public final Class<T> tClass;
    public final boolean sizeIsStaticallyKnown;
    private SizeMarshaller sizeMarshaller = stopBit();
    private SizedReader<T> reader;
    private DataAccess<T> dataAccess;

    @SuppressWarnings("unchecked")
    public SerializationBuilder(Class<T> tClass) {
        this.tClass = tClass;
        configureByDefault(tClass);
        sizeIsStaticallyKnown = constantSizeMarshaller();
    }

    private static boolean concreteClass(Class c) {
        return !c.isInterface() && !Modifier.isAbstract(c.getModifiers());
    }

    private static void checkNonMarshallableEnum(Class c) {
        if (Enum.class.isAssignableFrom(c) && (Marshallable.class.isAssignableFrom(c) ||
                ReadResolvable.class.isAssignableFrom(c))) {
            throw new IllegalArgumentException(c + ": since Chronicle Map 3.9.0, enum marshaller " +
                    "shouldn't be a Java enum and implement " + Marshallable.class.getName() +
                    " or " + ReadResolvable.class.getName() + ". There are problems with " +
                    "serializing/deserializing them in Chronicle Map header. Emulate enums by " +
                    "static final fields");
        }
    }

    @SuppressWarnings("unchecked")
    private void configureByDefault(Class<T> tClass) {
        if (tClass.isPrimitive()) {
            throw new IllegalArgumentException(
                    "Chronicle Map's key or value type cannot be primitive, " + tClass +
                            " type is given");
        }

        if (tClass.isInterface() && Values.isValueInterfaceOrImplClass(tClass)) {
            try {
                // Acquire a model before assigning readers/writers
                // if the interface is not a value interface
                ValueModel valueModel = ValueModel.acquire(tClass);
                reader((BytesReader<T>) new ValueReader<>(tClass));
                dataAccess(new ValueDataAccess<>(tClass));
                sizeMarshaller(constant((long) valueModel.sizeInBytes()));
                return;
            } catch (Exception e) {
                try {
                    tClass = Values.nativeClassFor(tClass);
                } catch (Exception ex) {
                    // ignore, fall through
                }
                // ignore, fall through
            }
        }

        if (concreteClass(tClass) && Byteable.class.isAssignableFrom(tClass)) {
            reader(new ByteableSizedReader<>((Class) tClass));
            dataAccess((DataAccess<T>) new ByteableDataAccess<>((Class) tClass));
            sizeMarshaller(constant(allocateByteable(tClass).maxSize()));
        } else if (tClass == CharSequence.class) {
            reader((SizedReader<T>) CharSequenceSizedReader.INSTANCE);
            dataAccess((DataAccess<T>) new CharSequenceUtf8DataAccess());
        } else if (tClass == StringBuilder.class) {
            reader((SizedReader<T>) StringBuilderSizedReader.INSTANCE);
            dataAccess((DataAccess<T>) new StringBuilderUtf8DataAccess());
        } else if (tClass == String.class) {
            reader((SizedReader<T>) new StringSizedReader());
            dataAccess((DataAccess<T>) new StringUtf8DataAccess());
        } else if (tClass == Boolean.class) {
            reader((SizedReader<T>) BooleanMarshaller.INSTANCE);
            notReusingWriter((SizedWriter<T>) BooleanMarshaller.INSTANCE);
            sizeMarshaller(constant(1));
        } else if (tClass == Long.class) {
            reader((SizedReader<T>) LongMarshaller.INSTANCE);
            dataAccess((DataAccess<T>) new LongDataAccess());
            sizeMarshaller(constant(8));
        } else if (tClass == Double.class) {
            reader((SizedReader<T>) DoubleMarshaller.INSTANCE);
            dataAccess((DataAccess<T>) new DoubleDataAccess());
            sizeMarshaller(constant(8));
        } else if (tClass == Integer.class) {
            reader((SizedReader<T>) IntegerMarshaller.INSTANCE);
            dataAccess((DataAccess<T>) new IntegerDataAccess_3_13());
            sizeMarshaller(constant(4));
        } else if (tClass == byte[].class) {
            reader((SizedReader<T>) ByteArraySizedReader.INSTANCE);
            dataAccess((DataAccess<T>) new ByteArrayDataAccess());
        } else if (tClass == ByteBuffer.class) {
            reader((SizedReader<T>) ByteBufferSizedReader.INSTANCE);
            dataAccess((DataAccess<T>) new ByteBufferDataAccess());
        } else if (concreteClass(tClass) && BytesMarshallable.class.isAssignableFrom(tClass)) {
            reader((BytesReader<T>) new BytesMarshallableReader<>((Class) tClass));
            dataAccess(new BytesMarshallableDataAccess<>((Class) tClass));
        } else if (concreteClass(tClass) && Externalizable.class.isAssignableFrom(tClass)) {
            reader((BytesReader<T>) new ExternalizableReader<>((Class) tClass));
            dataAccess(new ExternalizableDataAccess<>((Class) tClass));
        } else {
            reader((SizedReader<T>) new SerializableReader<>());
            dataAccess((DataAccess<T>) new SerializableDataAccess<>());
        }
    }

    @NotNull
    private Byteable allocateByteable(Class<T> tClass) {
        try {
            return (Byteable) OS.memory().allocateInstance(tClass);
        } catch (InstantiationException e) {
            throw new IllegalStateException(e);
        }
    }

    public void reader(SizedReader<T> reader) {
        checkNonMarshallableEnum(reader.getClass());
        this.reader = reader;
    }

    public void reader(BytesReader<T> reader) {
        checkNonMarshallableEnum(reader.getClass());
        this.reader = new BytesAsSizedReader<>(reader);
    }

    public SizedReader<T> reader() {
        return StatefulCopyable.copyIfNeeded(reader);
    }

    public void dataAccess(DataAccess<T> dataAccess) {
        checkNonMarshallableEnum(dataAccess.getClass());
        this.dataAccess = dataAccess;
    }

    public DataAccess<T> dataAccess() {
        return dataAccess.copy();
    }

    public void writer(SizedWriter<? super T> writer) {
        checkNonMarshallableEnum(writer.getClass());
        dataAccess(new SizedMarshallableDataAccess<>(tClass, reader, writer));
    }

    private void notReusingWriter(SizedWriter<? super T> writer) {
        checkNonMarshallableEnum(writer.getClass());
        dataAccess(new NotReusingSizedMarshallableDataAccess<>(tClass, reader, writer));
    }

    public void writer(BytesWriter<? super T> writer) {
        checkNonMarshallableEnum(writer.getClass());
        dataAccess(new ExternalBytesMarshallableDataAccess<>(tClass, reader, writer));
    }

    public long serializationSize(T sampleObject) {
        return dataAccess().getData(sampleObject).size();
    }

    public long constantSizeBySample(T sampleObject) {
        long constantSize = serializationSize(sampleObject);
        if (constantSizeMarshaller() && !DynamicallySized.class.isAssignableFrom(sampleObject.getClass())) {
            long expectedConstantSize = constantSize();
            if (constantSize != expectedConstantSize) {
                throw new IllegalStateException("Although configuring constant size by sample " +
                        "is not forbidden for types which size we already know statically, they " +
                        "should be the same. For " + tClass + " we know constant size is " +
                        expectedConstantSize + " statically, configured sample is " + sampleObject +
                        " which size in serialized form is " + constantSize);
            }
        }
        sizeMarshaller(constant(constantSize));
        return constantSize;
    }

    public SizeMarshaller sizeMarshaller() {
        return sizeMarshaller;
    }

    public boolean constantSizeMarshaller() {
        SizeMarshaller marshaller = sizeMarshaller();
        boolean feature1 = marshaller.storingLength(marshaller.maxStorableSize()) == 0;
        boolean feature2 = marshaller.minStorableSize() == marshaller.maxStorableSize();
        if (feature1 && feature2)
            return true;
        if (!feature1 && !feature2)
            return false;
        throw new IllegalStateException("SizeMarshaller " + marshaller + " has only 1 of 2 " +
                "constant features: storingLength == 0 and minStorableSize == maxStorableSize." +
                " Should have both");
    }

    public boolean constantStoringLengthSizeMarshaller() {
        SizeMarshaller marshaller = sizeMarshaller();
        long minStorableSize = marshaller.minStorableSize();
        long maxStorableSize = marshaller.maxStorableSize();
        return marshaller.minStoringLengthOfSizesInRange(minStorableSize, maxStorableSize) ==
                marshaller.maxStoringLengthOfSizesInRange(minStorableSize, maxStorableSize);
    }

    public long constantSize() {
        if (sizeMarshaller().minStorableSize() != sizeMarshaller().maxStorableSize())
            throw new AssertionError();
        return sizeMarshaller().minStorableSize();
    }

    public SerializationBuilder<T> sizeMarshaller(SizeMarshaller sizeMarshaller) {
        checkNonMarshallableEnum(sizeMarshaller.getClass());
        this.sizeMarshaller = sizeMarshaller;
        return this;
    }

    @Override
    public SerializationBuilder<T> clone() {
        try {
            //noinspection unchecked
            return (SerializationBuilder<T>) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }
}
