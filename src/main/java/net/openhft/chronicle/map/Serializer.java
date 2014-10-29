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
import net.openhft.lang.io.Bytes;
import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.constraints.NotNull;

/**
 * uses a {@code SerializationBuilder} to read and write object(s) as bytes
 *
 * @author Rob Austin.
 */
class Serializer<O> {

    final SizeMarshaller sizeMarshaller;
    final BytesReader<O> originalReader;
    transient Provider<BytesReader<O>> readerProvider;
    final Object originalWriter;
    transient Provider writerProvider;
    final MetaBytesInterop originalMetaValueWriter;
    final MetaProvider metaValueWriterProvider;

    Serializer(final SerializationBuilder serializationBuilder) {

        sizeMarshaller = serializationBuilder.sizeMarshaller();
        originalMetaValueWriter = serializationBuilder.metaInterop();
        metaValueWriterProvider = serializationBuilder.metaInteropProvider();

        originalReader = serializationBuilder.reader();
        originalWriter = serializationBuilder.interop();

        readerProvider = Provider.of((Class) originalReader.getClass());
        writerProvider = Provider.of((Class) originalWriter.getClass());
    }

    public O readMarshallable(@NotNull Bytes in) throws IllegalStateException {

        final ThreadLocalCopies copies = readerProvider.getCopies(null);
        final BytesReader<O> valueReader = readerProvider.get(copies, originalReader);

        try {
            long valueSize = sizeMarshaller.readSize(in);
            return valueReader.read(in, valueSize, null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void writeMarshallable(O value, @NotNull Bytes out) {

        ThreadLocalCopies copies = readerProvider.getCopies(null);
        Object valueWriter = writerProvider.get(copies, originalWriter);
        copies = writerProvider.getCopies(copies);

        final long valueSize;

        MetaBytesWriter metaValueWriter = null;

        if ((value instanceof Byteable)) {
            valueSize = ((Byteable) value).maxSize();
        } else {
            copies = writerProvider.getCopies(copies);
            valueWriter = writerProvider.get(copies, originalWriter);
            copies = metaValueWriterProvider.getCopies(copies);
            metaValueWriter = metaValueWriterProvider.get(
                    copies, originalMetaValueWriter, valueWriter, value);
            valueSize = metaValueWriter.size(valueWriter, value);
        }

        sizeMarshaller.writeSize(out, valueSize);

        if (metaValueWriter != null) {
            assert out.limit() == out.capacity();
            metaValueWriter.write(valueWriter, out, value);
        }  else
            throw new UnsupportedOperationException("");

    }

}
