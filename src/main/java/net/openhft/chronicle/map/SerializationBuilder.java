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

import net.openhft.lang.threadlocal.Provider;
import net.openhft.lang.threadlocal.ThreadLocalCopies;
import net.openhft.chronicle.hash.serialization.*;
import net.openhft.chronicle.hash.serialization.impl.*;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshallable;
import net.openhft.lang.io.serialization.BytesMarshaller;
import net.openhft.lang.io.serialization.ObjectFactory;
import net.openhft.lang.io.serialization.ObjectSerializer;
import net.openhft.lang.io.serialization.impl.*;
import net.openhft.lang.model.Byteable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Modifier;

import static net.openhft.chronicle.map.Objects.hash;
import static net.openhft.chronicle.hash.serialization.SizeMarshallers.stopBit;

final class SerializationBuilder<E> implements Cloneable {

    private static boolean concreteClass(Class c) {
        return !c.isInterface() && !Modifier.isAbstract(c.getModifiers());
    }

    private static boolean marshallerUseFactory(Class c) {
        return Byteable.class.isAssignableFrom(c) ||
                BytesMarshallable.class.isAssignableFrom(c) ||
                Externalizable.class.isAssignableFrom(c);
    }

    private static boolean instancesAreMutable(Class c) {
        return c != String.class;
    }

    enum Role {KEY, VALUE}

    private enum CopyingInterop {FROM_MARSHALLER, FROM_WRITER}

    private final Serializable bufferIdentity;
    final Class<E> eClass;
    private boolean instancesAreMutable;
    private SizeMarshaller sizeMarshaller = stopBit();
    private BytesReader<E> reader;
    private Object interop;
    private CopyingInterop copyingInterop = null;
    private MetaBytesInterop<E, ?> metaInterop;
    private MetaProvider<E, ?, ?> metaInteropProvider;
    private long maxSize;
    private ObjectFactory<E> factory;

    @SuppressWarnings("unchecked")
    SerializationBuilder(Class<E> eClass, Role role) {
        this.bufferIdentity = role;
        this.eClass = eClass;
        instancesAreMutable = instancesAreMutable(eClass);

        ObjectFactory<E> factory = concreteClass(eClass) && marshallerUseFactory(eClass) ?
                new NewInstanceObjectFactory<E>(eClass) :
                NullObjectFactory.<E>of();

        if (concreteClass(eClass) && Byteable.class.isAssignableFrom(eClass)) {
            agileMarshaller(ByteableMarshaller.of((Class) eClass), factory);
        } else if (eClass == CharSequence.class || eClass == String.class) {
            reader((BytesReader<E>) CharSequenceReader.of(), factory);
            writer((BytesWriter<E>) CharSequenceWriter.instance());
        } else if (eClass == Void.class) {
            agileMarshaller((AgileBytesMarshaller<E>) VoidMarshaller.INSTANCE, factory);
        } else if (eClass == Long.class) {
            agileMarshaller((AgileBytesMarshaller<E>) LongMarshaller.INSTANCE, factory);
        } else if (eClass == Double.class) {
            agileMarshaller((AgileBytesMarshaller<E>) DoubleMarshaller.INSTANCE, factory);
        } else if (eClass == Integer.class) {
            agileMarshaller((AgileBytesMarshaller<E>) IntegerMarshaller.INSTANCE, factory);
        } else if (eClass == byte[].class) {
            reader((BytesReader<E>) ByteArrayMarshaller.INSTANCE, factory);
            interop((BytesInterop<E>) ByteArrayMarshaller.INSTANCE);
        } else if (eClass == char[].class) {
            reader((BytesReader<E>) CharArrayMarshaller.INSTANCE, factory);
            interop((BytesInterop<E>) CharArrayMarshaller.INSTANCE);
        } else if (concreteClass(eClass)) {
            BytesMarshaller<E> marshaller = chooseMarshaller(eClass, eClass);
            if (marshaller != null)
                marshaller(marshaller, factory);
        }
        if (concreteClass(eClass) && marshallerUseFactory(eClass)) {
            factory(factory);
        }
    }

    @SuppressWarnings("unchecked")
    private BytesMarshaller<E> chooseMarshaller(Class<E> eClass, Class<E> classForMarshaller) {
        if (BytesMarshallable.class.isAssignableFrom(eClass))
            return new BytesMarshallableMarshaller(classForMarshaller);
        if (Externalizable.class.isAssignableFrom(eClass))
            return new ExternalizableMarshaller(classForMarshaller);
        return null;
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
                .setInterop(writer)
                .metaInterop(CopyingMetaBytesInterop
                        .<E, BytesWriter<E>>forBytesWriter(bufferIdentity));
    }

    public SerializationBuilder<E> marshaller(BytesMarshaller<E> marshaller,
                                              ObjectFactory<E> factory) {
        if (factory == null) factory = this.factory;
        return copyingInterop(CopyingInterop.FROM_MARSHALLER)
                .reader(BytesReaders.fromBytesMarshaller(marshaller), factory)
                .setInterop(marshaller)
                .metaInterop(CopyingMetaBytesInterop
                        .<E, BytesMarshaller<E>>forBytesMarshaller(bufferIdentity));
    }

    public SerializationBuilder<E> maxSize(long maxSize) {
        if (copyingInterop == CopyingInterop.FROM_MARSHALLER) {
            this.maxSize = maxSize;
            metaInteropProvider(CopyingMetaBytesInterop
                    .<E, BytesMarshaller<E>>providerForBytesMarshaller(
                            instancesAreMutable, maxSize));
        } else if (copyingInterop == CopyingInterop.FROM_WRITER) {
            this.maxSize = maxSize;
            metaInteropProvider(CopyingMetaBytesInterop
                    .<E, BytesWriter<E>>providerForBytesWriter(instancesAreMutable, maxSize));
        }
        return this;
    }

    public SerializationBuilder<E> constantSizeBySample(E sampleObject) {
        Object originalInterop = this.interop;
        Provider interopProvider = Provider.of(originalInterop.getClass());
        ThreadLocalCopies copies = interopProvider.getCopies(null);
        Object interop = interopProvider.get(copies, originalInterop);
        copies = metaInteropProvider.getCopies(copies);
        MetaBytesWriter metaInterop;
        if (maxSize > 0) {
            // this loop is very dumb: tries to find buffer size, sufficient to serialize
            // the object, by trying to serialize to small buffers first and doubling the buffer
            // size on _any_ Exception (it may be IllegalArgument, IndexOutOfBounds, IllegalState,
            // see JLANG-17 issue).
            // TODO The question is: couldn't this cause JVM crash without throwing an exception?
            findSufficientSerializationSize:
            while (true) {
                MetaProvider metaInteropProvider = this.metaInteropProvider;
                try {
                    metaInterop = metaInteropProvider.get(
                            copies, this.metaInterop, interop, sampleObject);
                    break findSufficientSerializationSize;
                } catch (Exception e) {
                    // assuming nobody need > 512 mb _constant size_ keys/values
                    if (maxSize > 512 * 1024 * 1024) {
                        throw e;
                    }
                    maxSize(maxSize * 2);
                }
            }
        } else {
            MetaProvider metaInteropProvider = this.metaInteropProvider;
            metaInterop = metaInteropProvider.get(copies, this.metaInterop, interop, sampleObject);
        }
        long constantSize = metaInterop.size(interop, sampleObject);
        // set max size once more, because current maxSize could be x64 or x2 larger than needed
        maxSize(constantSize);
        sizeMarshaller(SizeMarshallers.constant(constantSize));
        return this;
    }

    public SerializationBuilder<E> objectSerializer(ObjectSerializer serializer) {
        if (reader == null || interop == null) {
            ObjectFactory<E> factory = this.factory;
            if (factory == null)
                factory = NullObjectFactory.INSTANCE;
            marshaller(new SerializableMarshaller(serializer), factory);
        }
        return this;
    }

    public SerializationBuilder<E> instancesAreMutable(boolean mutable) {
        this.instancesAreMutable = mutable;
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

    private static class SerializableMarshaller implements BytesMarshaller {
        private static final long serialVersionUID = 0L;

        private final ObjectSerializer serializer;

        private SerializableMarshaller(ObjectSerializer serializer) {
            this.serializer = serializer;
        }

        @Override
        public void write(Bytes bytes, Object obj) {
            try {
                serializer.writeSerializable(bytes, obj, null);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Nullable
        @Override
        public Object read(Bytes bytes) {
            try {
                return serializer.readSerializable(bytes, null, null);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e);
            }
        }

        @Nullable
        @Override
        public Object read(Bytes bytes, @Nullable Object obj) {
            try {
                return serializer.readSerializable(bytes, null, obj);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e);
            }
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
