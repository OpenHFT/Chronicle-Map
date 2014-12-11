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

import net.openhft.chronicle.hash.serialization.*;
import net.openhft.chronicle.hash.serialization.internal.*;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshallable;
import net.openhft.lang.io.serialization.BytesMarshaller;
import net.openhft.lang.io.serialization.ObjectFactory;
import net.openhft.lang.io.serialization.ObjectSerializer;
import net.openhft.lang.io.serialization.impl.*;
import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.DataValueGenerator;
import net.openhft.lang.model.DataValueModel;
import net.openhft.lang.model.DataValueModels;
import net.openhft.lang.threadlocal.Provider;
import net.openhft.lang.threadlocal.ThreadLocalCopies;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static net.openhft.chronicle.hash.serialization.SizeMarshallers.constant;
import static net.openhft.chronicle.hash.serialization.SizeMarshallers.stopBit;
import static net.openhft.chronicle.map.Objects.hash;

final class SerializationBuilder<E> implements Cloneable, Serializable {

    private static final Bytes EMPTY_BYTES = new ByteBufferBytes(ByteBuffer.allocate(0));

    private static boolean concreteClass(Class c) {
        return !c.isInterface() && !Modifier.isAbstract(c.getModifiers());
    }

    private static boolean marshallerUseFactory(Class c) {
        return Byteable.class.isAssignableFrom(c) ||
                BytesMarshallable.class.isAssignableFrom(c) ||
                Externalizable.class.isAssignableFrom(c);
    }

    private static final List<Class> knownJDKImmutableClasses = Arrays.<Class>asList(
            String.class, Byte.class, Short.class, Character.class, Integer.class,
            Float.class, Long.class, Double.class, BigDecimal.class, BigInteger.class, URL.class
    );

    private static boolean instancesAreMutable(Class c) {
        return !knownJDKImmutableClasses.contains(c);
    }

    enum Role {KEY, VALUE}

    private enum CopyingInterop {FROM_MARSHALLER, FROM_WRITER}

    private final Role role;
    final Class<E> eClass;
    private boolean instancesAreMutable;
    private SizeMarshaller sizeMarshaller = stopBit();
    private BytesReader<E> reader;
    private Object interop;
    private CopyingInterop copyingInterop = null;
    private MetaBytesInterop<E, ?> metaInterop;
    private MetaProvider<E, ?, ?> metaInteropProvider;
    private long maxSize;

    final boolean sizeIsStaticallyKnown;

    @SuppressWarnings("unchecked")
    SerializationBuilder(Class<E> eClass, Role role) {
        this.role = role;
        this.eClass = eClass;
        configureByDefault(eClass, role);
        sizeIsStaticallyKnown = constantSizeMarshaller();
    }

    private void configureByDefault(Class<E> eClass, Role role) {
        instancesAreMutable = instancesAreMutable(eClass);

        ObjectFactory<E> factory = concreteClass(eClass) && marshallerUseFactory(eClass) ?
                new NewInstanceObjectFactory<E>(eClass) :
                NullObjectFactory.<E>of();

        if (eClass.isInterface()) {
            try {
                BytesReader<E> reader = DataValueBytesMarshallers.acquireBytesReader(eClass);
                BytesWriter<E> writer = DataValueBytesMarshallers.acquireBytesWriter(eClass);
                DataValueModel<E> model = DataValueModels.acquireModel(eClass);
                int size = DataValueGenerator.computeNonScalarOffset(model, eClass);
                reader(reader);
                writer(writer);
                sizeMarshaller(constant((long) size));
                return;
            } catch (Exception e) {
                // ignore, fall through
            }
        }

        if (concreteClass(eClass) && Byteable.class.isAssignableFrom(eClass)) {
            ByteableMarshaller byteableMarshaller = ByteableMarshaller.of((Class) eClass);
            sizeMarshaller(byteableMarshaller);
            reader(byteableMarshaller);
            interop(byteableMarshaller);
        } else if (eClass == CharSequence.class || eClass == String.class) {
            reader((BytesReader<E>) CharSequenceReader.of());
            writer((BytesWriter<E>) CharSequenceWriter.instance());
        } else if (eClass == Void.class) {
            sizeMarshaller(VoidMarshaller.INSTANCE);
            reader((BytesReader<E>) VoidMarshaller.INSTANCE);
            interop((BytesInterop<E>) VoidMarshaller.INSTANCE);
        } else if (eClass == Long.class) {
            sizeMarshaller(LongMarshaller.INSTANCE);
            reader((BytesReader<E>) LongMarshaller.INSTANCE);
            interop((BytesInterop<E>) LongMarshaller.INSTANCE);
        } else if (eClass == Double.class) {
            sizeMarshaller(DoubleMarshaller.INSTANCE);
            reader((BytesReader<E>) DoubleMarshaller.INSTANCE);
            interop((BytesInterop<E>) DoubleMarshaller.INSTANCE);
        } else if (eClass == Integer.class) {
            sizeMarshaller(IntegerMarshaller.INSTANCE);
            reader((BytesReader<E>) IntegerMarshaller.INSTANCE);
            interop((BytesInterop<E>) IntegerMarshaller.INSTANCE);
        } else if (eClass == byte[].class) {
            reader((BytesReader<E>) ByteArrayMarshaller.INSTANCE);
            interop((BytesInterop<E>) ByteArrayMarshaller.INSTANCE);
        } else if (eClass == char[].class) {
            reader((BytesReader<E>) CharArrayMarshaller.INSTANCE);
            interop((BytesInterop<E>) CharArrayMarshaller.INSTANCE);
        } else if (concreteClass(eClass)) {
            BytesMarshaller<E> marshaller = chooseMarshaller(eClass, eClass);
            if (marshaller != null)
                marshaller(marshaller);
        }
        if (concreteClass(eClass) && marshallerUseFactory(eClass)) {
            factory(factory);
        }
    }

    boolean possibleOffHeapReferences() {
        if (reader instanceof CharSequenceReader)
            return false;
        if (reader instanceof VoidMarshaller)
            return false;
        // exclude some known classes, most notably boxed primitive types
        if (!instancesAreMutable(eClass))
            return false;
        if (eClass.isArray() && (eClass.getComponentType().isPrimitive() ||
                instancesAreMutable(eClass.getComponentType())))
            return false;
        BytesMarshaller<E> marshallerAsReader = BytesReaders.getBytesMarshaller(reader);
        if (marshallerAsReader instanceof SerializableMarshaller ||
                marshallerAsReader instanceof ExternalizableMarshaller)
            return false;
        // otherwise reader could possibly keep the given bytes addresses and update off-heap memory
        return true;
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

    public SerializationBuilder<E> interop(BytesInterop<E> interop) {
        return copyingInterop(null)
                .setInterop(interop)
                .metaInterop(DelegatingMetaBytesInterop.<E, BytesInterop<E>>instance())
                .metaInteropProvider(DelegatingMetaBytesInteropProvider
                        .<E, BytesInterop<E>>instance());
    }

    public SerializationBuilder<E> writer(BytesWriter<E> writer) {
        if (writer instanceof BytesInterop)
            return interop((BytesInterop<E>) writer);
        return copyingInterop(CopyingInterop.FROM_WRITER)
                .setInterop(writer)
                .metaInterop(CopyingMetaBytesInterop
                        .<E, BytesWriter<E>>forBytesWriter(role));
    }

    public SerializationBuilder<E> marshaller(BytesMarshaller<? super E> marshaller) {
        return copyingInterop(CopyingInterop.FROM_MARSHALLER)
                .reader(BytesReaders.fromBytesMarshaller(marshaller))
                .setInterop(marshaller)
                .metaInterop(CopyingMetaBytesInterop
                        .<E, BytesMarshaller<E>>forBytesMarshaller(role));
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
        if (sizeIsStaticallyKnown) {
            int expectedConstantSize = pseudoReadConstantSize();
            if (constantSize != expectedConstantSize) {
                throw new IllegalStateException("Although configuring constant size by sample " +
                        "is not forbidden for types which size we already know statically, they " +
                        "should be the same. For " + eClass + " we know constant size is " +
                        expectedConstantSize + " statically, configured sample is " + sampleObject +
                        " which size in serialized form is " + constantSize);
            }
        }
        // set max size once more, because current maxSize could be x64 or x2 larger than needed
        maxSize(constantSize);
        sizeMarshaller(SizeMarshallers.constant(constantSize));
        return this;
    }

    public SerializationBuilder<E> objectSerializer(ObjectSerializer serializer) {
        if (reader == null || interop == null) {
            marshaller(new SerializableMarshaller(serializer));
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

    boolean constantSizeMarshaller() {
        return sizeMarshaller().sizeEncodingSize(0L) == 0;
    }

    int pseudoReadConstantSize() {
        return (int) sizeMarshaller().readSize(EMPTY_BYTES);
    }

    public SerializationBuilder<E> sizeMarshaller(SizeMarshaller sizeMarshaller) {
        this.sizeMarshaller = sizeMarshaller;
        return this;
    }

    public BytesReader<E> reader() {
        return reader;
    }

    public SerializationBuilder<E> reader(BytesReader<E> reader) {
        this.reader = reader;
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

    @SuppressWarnings("unchecked")
    public SerializationBuilder<E> factory(ObjectFactory<E> factory) {
        if (reader instanceof DeserializationFactoryConfigurableBytesReader) {
            DeserializationFactoryConfigurableBytesReader newReader =
                    ((DeserializationFactoryConfigurableBytesReader) reader)
                            .withDeserializationFactory(factory);
            reader(newReader);
            if (newReader instanceof BytesInterop)
                interop((BytesInterop<E>) newReader);
            if (newReader instanceof SizeMarshaller)
                sizeMarshaller((SizeMarshaller) newReader);
            return this;
        }
        if (!marshallerUseFactory(eClass)) {
            throw new IllegalStateException("Default marshaller for " + eClass +
                    " value don't use object factory");
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
            String role = this.role.toString().toLowerCase();
            throw new IllegalStateException(
                    role + "DeserializationFactory() should be called " +
                            "only if the Map " + role + " type is Byteable, BytesMarshallable " +
                            "or Externalizable subtype and no custom marshallers were configured.");
        }
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
