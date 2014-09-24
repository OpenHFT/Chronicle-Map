/*
 * Copyright 2014 Higher Frequency Trading http://www.higherfrequencytrading.com
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

import net.openhft.chronicle.map.serialization.*;
import net.openhft.chronicle.map.serialization.impl.*;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.*;
import net.openhft.lang.io.serialization.impl.*;
import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.DataValueClasses;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Externalizable;
import java.io.Serializable;

import static net.openhft.chronicle.map.Objects.hash;
import static net.openhft.chronicle.map.serialization.SizeMarshallers.stopBit;

final class SerializationBuilder<E> implements Cloneable {
    
    private static boolean marshallerUseFactory(Class c) {
        return Byteable.class.isAssignableFrom(c) ||
                BytesMarshallable.class.isAssignableFrom(c) ||
                Externalizable.class.isAssignableFrom(c);
    }

    enum Role {KEY, VALUE}
    private enum CopyingInterop {FROM_MARSHALLER, FROM_WRITER}

    private final Serializable bufferIdentity;
    final Class<E> eClass;
    private SizeMarshaller sizeMarshaller = stopBit();
    private BytesReader<E> reader;
    private Object interop;
    private CopyingInterop copyingInterop = null;
    private MetaBytesInterop<E, ?> metaInterop;
    private MetaProvider<E, ?, ?> metaInteropProvider;
    private  ObjectFactory<E> factory;

    @SuppressWarnings("unchecked")
    SerializationBuilder(Class<E> eClass, Role role) {
        this.bufferIdentity = role;
        this.eClass = eClass;
        Class<E> classForMarshaller = marshallerUseFactory(eClass) && eClass.isInterface() ?
                DataValueClasses.directClassFor(eClass) : eClass;

        ObjectFactory<E> factory = null;
        if (role == Role.VALUE) {
            factory = marshallerUseFactory(eClass) ?
                    new AllocateInstanceObjectFactory(
                            eClass.isInterface() ? classForMarshaller : eClass) :
                    NullObjectFactory.INSTANCE;
        }

        if (Byteable.class.isAssignableFrom(eClass)) {
            agileMarshaller(ByteableMarshaller.of((Class) classForMarshaller), factory);
        } else if (eClass == CharSequence.class || eClass == String.class) {
            reader((BytesReader<E>) CharSequenceReader.of(), factory);
            writer((BytesWriter<E>) CharSequenceWriter.instance());
        } else if (eClass == Long.class) {
            agileMarshaller((AgileBytesMarshaller<E>) LongMarshaller.INSTANCE, factory);
        } else if (eClass == byte[].class) {
            reader((BytesReader<E>) ByteArrayMarshaller.INSTANCE, factory);
            interop((BytesInterop<E>) ByteArrayMarshaller.INSTANCE);
        } else {
            marshaller(chooseMarshaller(eClass, classForMarshaller), factory);
        }
    }

    @SuppressWarnings("unchecked")
    private BytesMarshaller<E> chooseMarshaller(Class<E> eClass, Class<E> classForMarshaller) {
        if (BytesMarshallable.class.isAssignableFrom(eClass))
            return new BytesMarshallableMarshaller(classForMarshaller);
        if (Externalizable.class.isAssignableFrom(eClass))
            return new ExternalizableMarshaller(classForMarshaller);
        if (eClass == Integer.class)
            return (BytesMarshaller<E>) IntegerMarshaller.INSTANCE;
        if (eClass == Double.class)
            return (BytesMarshaller<E>) DoubleMarshaller.INSTANCE;
        return SerializableMarshaller.INSTANCE;
    }

    private SerializationBuilder<E> copyingInterop(CopyingInterop copyingInterop) {
        this.copyingInterop = copyingInterop;
        return this;
    }

    public SerializationBuilder<E> agileMarshaller(AgileBytesMarshaller<E> agileBytesMarshaller,
                                                   ObjectFactory<E> factory) {
        if (factory == null) factory = this.factory;
        return sizeMarshaller(agileBytesMarshaller)
                .reader(agileBytesMarshaller, factory)
                .interop(agileBytesMarshaller);
    }

    public SerializationBuilder<E> interop(BytesInterop<E> interop) {
        return copyingInterop(null)
                .setInterop(interop)
                .metaInterop(DelegatingMetaBytesInterop.<E, BytesInterop<E>>instance())
                .metaInteropProvider(DelegatingMetaBytesInteropProvider
                        .<E, BytesInterop<E>>instance());
    }

    public SerializationBuilder<E> writer(BytesWriter<E> writer) {
        return copyingInterop(CopyingInterop.FROM_WRITER)
                .sizeMarshaller(stopBit())
                .setInterop(writer)
                .metaInterop(CopyingMetaBytesInterop
                        .<E, BytesWriter<E>>forBytesWriter(bufferIdentity));
    }

    public SerializationBuilder<E> marshaller(BytesMarshaller<E> marshaller,
                                              ObjectFactory<E> factory) {
        if (factory == null) factory = this.factory;
        return copyingInterop(CopyingInterop.FROM_MARSHALLER)
                .sizeMarshaller(stopBit())
                .reader(BytesReaders.fromBytesMarshaller(marshaller), factory)
                .setInterop(marshaller)
                .metaInterop(CopyingMetaBytesInterop
                        .<E, BytesMarshaller<E>>forBytesMarshaller(bufferIdentity));
    }

    public SerializationBuilder<E> maxSize(long maxSize) {
        boolean mutable = eClass != String.class; // for example
        if (copyingInterop == CopyingInterop.FROM_MARSHALLER) {
            metaInteropProvider(CopyingMetaBytesInterop
                    .<E, BytesMarshaller<E>>providerForBytesMarshaller(mutable, maxSize));
        } else if (copyingInterop == CopyingInterop.FROM_WRITER) {
            metaInteropProvider(CopyingMetaBytesInterop
                    .<E, BytesWriter<E>>providerForBytesWriter(mutable, maxSize));
        }
        return this;
    }

    public SizeMarshaller sizeMarshaller() {
        return sizeMarshaller;
    }

    public SerializationBuilder<E> sizeMarshaller(SizeMarshaller sizeMarshaller) {
        this.sizeMarshaller = sizeMarshaller;
        return this;
    }

    public BytesReader<E> reader() {
        return reader;
    }

    public SerializationBuilder<E> reader(BytesReader<E> reader, ObjectFactory<E> factory) {
        this.reader = reader;
        if (factory != null)
            this.factory = factory;
        return this;

    }

    public Object interop() {
        return interop;
    }

    private SerializationBuilder<E> setInterop(Object interop) {
        this.interop = interop;
        return this;
    }

    public MetaBytesInterop<E, ?> metaInterop() {
        return metaInterop;
    }

    private SerializationBuilder<E> metaInterop(MetaBytesInterop<E, ?> metaInterop) {
        this.metaInterop = metaInterop;
        return this;
    }

    @SuppressWarnings("unchecked")
    public MetaProvider<E, ?, MetaBytesInterop<E, ?>> metaInteropProvider() {
        return (MetaProvider<E, ?, MetaBytesInterop<E, ?>>) metaInteropProvider;
    }

    private SerializationBuilder<E> metaInteropProvider(
            MetaProvider<E, ?, ?> metaInteropProvider) {
        this.metaInteropProvider = metaInteropProvider;
        return this;
    }

    public ObjectFactory<E> factory() {
        return factory;
    }

    @SuppressWarnings("unchecked")
    public SerializationBuilder<E> factory(ObjectFactory<E> factory) {
        if (!marshallerUseFactory(eClass)) {
            throw new IllegalStateException("Default marshaller for " + eClass +
                    " value don't use object factory");
        } else if (interop instanceof ByteableMarshaller) {
            if (factory instanceof AllocateInstanceObjectFactory) {
                agileMarshaller(ByteableMarshaller.of((Class) eClass), factory);
            } else {
                agileMarshaller(ByteableMarshaller.of((Class) eClass, (ObjectFactory) factory),
                        factory);
            }
        } else if (interop instanceof BytesMarshallableMarshaller) {
            if (factory instanceof AllocateInstanceObjectFactory) {
                interop = new BytesMarshallableMarshaller(
                        ((AllocateInstanceObjectFactory) factory).allocatedClass());
            } else {
                interop = new BytesMarshallableMarshallerWithCustomFactory(
                        ((BytesMarshallableMarshaller) interop).marshaledClass(),
                        factory
                );
            }
        } else if (interop instanceof ExternalizableMarshaller) {
            if (factory instanceof AllocateInstanceObjectFactory) {
                interop = new ExternalizableMarshaller(
                        ((AllocateInstanceObjectFactory) factory).allocatedClass());
            } else {
                interop = new ExternalizableMarshallerWithCustomFactory(
                        ((ExternalizableMarshaller) interop).marshaledClass(),
                        factory
                );
            }
        } else {
            // interop is custom, it is user's responsibility to use the same factory inside
            // marshaller and standalone
            throw new IllegalStateException(
                    "Change the value factory simultaneously with marshaller " +
                            "using *AndFactory() method");
        }
        this.factory = factory;
        return this;
    }

    @Override
    public SerializationBuilder<E> clone() {
        try {
            return (SerializationBuilder<E>) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

    private enum IntegerMarshaller implements BytesMarshaller<Integer> {
        INSTANCE;

        @Override
        public void write(Bytes bytes, Integer v) {
            bytes.writeInt(v);
        }

        @Nullable
        @Override
        public Integer read(Bytes bytes) {
            return bytes.readInt();
        }

        @Nullable
        @Override
        public Integer read(Bytes bytes, @Nullable Integer v) {
            return bytes.readInt();
        }
    }

    private enum DoubleMarshaller implements BytesMarshaller<Double> {
        INSTANCE;

        @Override
        public void write(Bytes bytes, Double v) {
            bytes.writeDouble(v);
        }

        @Nullable
        @Override
        public Double read(Bytes bytes) {
            return bytes.readDouble();
        }

        @Nullable
        @Override
        public Double read(Bytes bytes, @Nullable Double v) {
            return bytes.readDouble();
        }
    }


    private enum SerializableMarshaller implements BytesMarshaller {
        INSTANCE;

        @Override
        public void write(Bytes bytes, Object obj) {
            bytes.writeObject(obj);
        }

        @Nullable
        @Override
        public Object read(Bytes bytes) {
            return bytes.readObject();
        }

        @Nullable
        @Override
        public Object read(Bytes bytes, @Nullable Object obj) {
            return bytes.readInstance(null, obj);
        }
    }

    private static class BytesMarshallableMarshallerWithCustomFactory<T extends BytesMarshallable>
            extends BytesMarshallableMarshaller<T> {
        private static final long serialVersionUID = 0L;

        @NotNull
        private final ObjectFactory<T> factory;

        BytesMarshallableMarshallerWithCustomFactory(@NotNull Class<T> tClass,
                                                     @NotNull ObjectFactory<T> factory) {
            super(tClass);
            this.factory = factory;
        }

        @NotNull
        @Override
        protected T getInstance() throws Exception {
            return factory.create();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != getClass())
                return false;
            BytesMarshallableMarshallerWithCustomFactory that =
                    (BytesMarshallableMarshallerWithCustomFactory) obj;
            return that.marshaledClass() == marshaledClass() && that.factory.equals(this.factory);
        }

        @Override
        public int hashCode() {
            return hash(marshaledClass(), factory);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{marshaledClass=" + marshaledClass() +
                    ",factory=" + factory + "}";
        }
    }

    private static class ExternalizableMarshallerWithCustomFactory<T extends Externalizable>
            extends ExternalizableMarshaller<T> {
        private static final long serialVersionUID = 0L;

        @NotNull
        private final ObjectFactory<T> factory;

        ExternalizableMarshallerWithCustomFactory(@NotNull Class<T> tClass,
                                                  @NotNull ObjectFactory<T> factory) {
            super(tClass);
            this.factory = factory;
        }

        @NotNull
        @Override
        protected T getInstance() throws Exception {
            return factory.create();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != getClass())
                return false;
            ExternalizableMarshallerWithCustomFactory that =
                    (ExternalizableMarshallerWithCustomFactory) obj;
            return that.marshaledClass() == marshaledClass() && that.factory.equals(this.factory);
        }

        @Override
        public int hashCode() {
            return hash(marshaledClass(), factory);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{marshaledClass=" + marshaledClass() +
                    ",factory=" + factory + "}";
        }
    }
}
