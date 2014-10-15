package net.openhft.chronicle.map;

import net.openhft.chronicle.map.serialization.BytesReader;
import net.openhft.chronicle.map.serialization.MetaBytesWriter;
import net.openhft.chronicle.map.serialization.MetaProvider;
import net.openhft.chronicle.map.serialization.SizeMarshaller;
import net.openhft.chronicle.map.threadlocal.Provider;
import net.openhft.chronicle.map.threadlocal.ThreadLocalCopies;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshallable;
import net.openhft.lang.io.serialization.ObjectFactory;
import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.constraints.NotNull;

/**
 * uses a {@code SerializationBuilder} to readMarshallable and writeMarshallable objects
 * @author Rob Austin.
 */
public class Serializer<O, VW, MVW extends MetaBytesWriter<O, VW>>
        implements BytesMarshallable {

    final Class<O> clazz;
    final SizeMarshaller sizeMarshaller;
    final BytesReader<O> originalReader;
    transient Provider<BytesReader<O>> readerProvider;
    final VW originalWriter;
    transient Provider<VW> writerProvider;
    final MVW originalMetaValueWriter;
    final MetaProvider<O, VW, MVW> metaValueWriterProvider;
    final ObjectFactory<O> objectFactory;

    final ThreadLocal<O> value = new ThreadLocal<O>();

    public Serializer(final SerializationBuilder<O> serializationBuilder) {

        clazz = serializationBuilder.eClass;
        sizeMarshaller = serializationBuilder.sizeMarshaller();
        originalReader = serializationBuilder.reader();

        originalWriter = (VW) serializationBuilder.interop();
        originalMetaValueWriter = (MVW) serializationBuilder.metaInterop();
        metaValueWriterProvider = (MetaProvider) serializationBuilder.metaInteropProvider();
        objectFactory = serializationBuilder.factory();

        readerProvider = Provider.of((Class) originalReader.getClass());
        writerProvider = Provider.of((Class) originalWriter.getClass());
    }


    public O getObject() {
        return value.get();
    }

    public void setObject(O value) {
        this.value.set(value);
    }

    @Override
    public void readMarshallable(@NotNull Bytes in) throws IllegalStateException {
        O o = readObject(in);
        setObject(o);
    }

    @Override
    public void writeMarshallable(@NotNull Bytes out) {
        O value1 = getObject();
        boolean byteableValue = value1 instanceof Byteable;
        ThreadLocalCopies copies;
        copies = readerProvider.getCopies(null);
        VW valueWriter = writerProvider.get(copies, originalWriter);
        copies = writerProvider.getCopies(copies);
        long valueSize;
        MetaBytesWriter<O, VW> metaValueWriter = null;
        Byteable valueAsByteable = null;

        if (!byteableValue) {
            copies = writerProvider.getCopies(copies);
            valueWriter = writerProvider.get(copies, originalWriter);
            copies = metaValueWriterProvider.getCopies(copies);
            metaValueWriter = metaValueWriterProvider.get(
                    copies, originalMetaValueWriter, valueWriter, value1);
            valueSize = metaValueWriter.size(valueWriter, value1);
        } else {
            valueAsByteable = (Byteable) value1;
            valueSize = valueAsByteable.maxSize();
        }

        sizeMarshaller.writeSize(out, valueSize);


        if (metaValueWriter != null) {
            metaValueWriter.write(valueWriter, out, value1);
        } else {
            throw new UnsupportedOperationException("");
        }
    }

    private O readObject(Bytes in) {

        ThreadLocalCopies copies;
        copies = readerProvider.getCopies(null);
        BytesReader<O> valueReader = readerProvider.get(copies, originalReader);

        try {
            final O v = objectFactory.create();
            long valueSize = sizeMarshaller.readSize(in);
            return valueReader.read(in, valueSize, v);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
