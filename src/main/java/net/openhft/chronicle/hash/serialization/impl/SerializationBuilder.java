/*
 *      Copyright (C) 2015  higherfrequencytrading.com
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Byteable;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.hash.serialization.*;
import net.openhft.chronicle.values.ValueModel;
import net.openhft.chronicle.values.Values;

import java.io.*;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;

import static net.openhft.chronicle.hash.serialization.SizeMarshaller.constant;
import static net.openhft.chronicle.hash.serialization.SizeMarshaller.stopBit;

public final class SerializationBuilder<T> implements Cloneable, Serializable {
    private static final long serialVersionUID = 0L;

    private static boolean concreteClass(Class c) {
        return !c.isInterface() && !Modifier.isAbstract(c.getModifiers());
    }

    public final Class<T> tClass;
    private SizeMarshaller sizeMarshaller = stopBit();
    private SizedReader<T> reader;
    private DataAccess<T> dataAccess;

    public final boolean sizeIsStaticallyKnown;
    
    boolean serializesGeneratedClasses = false;

    @SuppressWarnings("unchecked")
    public SerializationBuilder(Class<T> tClass) {
        this.tClass = tClass;
        configureByDefault(tClass);
        sizeIsStaticallyKnown = constantSizeMarshaller();
    }

    @SuppressWarnings("unchecked")
    private void configureByDefault(Class<T> tClass) {

        if (tClass.isInterface()) {
            try {
                // Acquire a model before assigning readers/writers
                // if the interface is not
                ValueModel valueModel = ValueModel.acquire(tClass);
                reader((BytesReader<T>) new ValueReader<>(tClass));
                dataAccess(new ValueDataAccess<>(tClass));
                sizeMarshaller(constant((long) valueModel.sizeInBytes()));
                serializesGeneratedClasses = true;
                return;
            } catch (Exception e) {
                try {
                    tClass = Values.nativeClassFor(tClass);
                    serializesGeneratedClasses = true;
                } catch (Exception ex) {
                    // ignore, fall through
                }
                // ignore, fall through
            }
        }

        if (concreteClass(tClass) && Byteable.class.isAssignableFrom(tClass)) {
            reader(new ByteableSizedReader<>((Class) tClass));
            dataAccess((DataAccess<T>) new ByteableDataAccess<>((Class) tClass));
            sizeMarshaller(constant(((Byteable) OS.memory().allocateInstance(tClass)).maxSize()));
        } else if (tClass == CharSequence.class) {
            reader((SizedReader<T>) CharSequenceSizedReader.INSTANCE);
            writer((SizedWriter<T>) CharSequenceSizedWriter.INSTANCE);
        } else if (tClass == StringBuilder.class) {
            reader((SizedReader<T>) StringBuilderSizedReader.INSTANCE);
            writer((SizedWriter<T>) CharSequenceSizedWriter.INSTANCE);
        } else if (tClass == String.class) {
            reader((SizedReader<T>) new StringSizedReader());
            notReusingWriter((SizedWriter<T>) CharSequenceSizedWriter.INSTANCE);
        } else if (tClass == Boolean.class) {
            reader((SizedReader<T>) BooleanMarshaller.INSTANCE);
            notReusingWriter((SizedWriter<T>) BooleanMarshaller.INSTANCE);
            sizeMarshaller(constant(1));
        } else if (tClass == Long.class) {
            reader((SizedReader<T>) LongMarshaller.INSTANCE);
            notReusingWriter((SizedWriter<T>) LongMarshaller.INSTANCE);
            sizeMarshaller(constant(8));
        } else if (tClass == Double.class) {
            reader((SizedReader<T>) DoubleMarshaller.INSTANCE);
            notReusingWriter((SizedWriter<T>) DoubleMarshaller.INSTANCE);
            sizeMarshaller(constant(8));
        } else if (tClass == Integer.class) {
            reader((SizedReader<T>) IntegerMarshaller.INSTANCE);
            notReusingWriter((SizedWriter<T>) IntegerMarshaller.INSTANCE);
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

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.writeBoolean(serializesGeneratedClasses);
        if (serializesGeneratedClasses) {
            out.writeObject(tClass);
        }
        out.defaultWriteObject();
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        boolean generatedClassesSerialized = in.readBoolean();
        if (generatedClassesSerialized) {
            Class<?> eClass = (Class) in.readObject();
            // This is needed because ValueDataAccess/ValueReader tries to serialize/deserialize
            // generated classes, requires them to be generated beforehand
            new SerializationBuilder<>(eClass);
        }
        in.defaultReadObject();
    }

    public void reader(SizedReader<T> reader) {
        this.reader = reader;
    }

    public void reader(BytesReader<T> reader) {
        this.reader = new BytesAsSizedReader<>(reader);
    }

    public SizedReader<T> reader() {
        return StatefulCopyable.copyIfNeeded(reader);
    }

    public void dataAccess(DataAccess<T> dataAccess) {
        this.dataAccess = dataAccess;
    }

    public DataAccess<T> dataAccess() {
        return dataAccess.copy();
    }

    public void writer(SizedWriter<? super T> writer) {
        dataAccess(new SizedMarshallableDataAccess<>(tClass, reader, writer));
    }

    private void notReusingWriter(SizedWriter<? super T> writer) {
        dataAccess(new NotReusingSizedMarshallableDataAccess<>(tClass, reader, writer));
    }

    public void writer(BytesWriter<? super T> writer) {
        dataAccess(new ExternalBytesMarshallableDataAccess<>(tClass, reader, writer));
    }

    public long serializationSize(T sampleObject) {
        return dataAccess().getData(sampleObject).size();
    }

    public long constantSizeBySample(T sampleObject) {
        long constantSize = serializationSize(sampleObject);
        if (constantSizeMarshaller()) {
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
