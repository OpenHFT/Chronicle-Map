/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.hash.serialization.internal;

import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshaller;

/**
 * Utility methods returning {@link BytesReader} implementations.
 */
public final class BytesReaders {

    /**
     * Returns a {@link BytesReader} wrapping the given {@link BytesMarshaller}. One of the
     * bridge methods between general serialization API from {@link net.openhft.lang} and
     * reader - writer - interop API, specific for ChronicleHashes,
     * from {@link net.openhft.chronicle.hash.serialization} package.
     *
     * @param marshaller the actual reading implementation
     * @param <E>        type of the objects marshalled
     * @return a {@code BytesReader} wrapping the given {@code BytesMarshaller}
     */
    public static <E> BytesReader<E> fromBytesMarshaller(BytesMarshaller<? super E> marshaller) {
        return new SimpleBytesReader<E>(marshaller);
    }

    /**
     * Returns {@code BytesMarshaller} {@code m}, if the given reader is the result of {@link
     * #fromBytesMarshaller(BytesMarshaller) fromBytesMarshaller(m)} call, {@code null} otherwise.
     *
     * @param reader reader to extract {@code BytesMarshaller} from
     * @param <E> type of the objects marshalled
     * @return {@code BytesMarshaller} from which the specified reader was created, or {@code null}
     */
    public static <E> BytesMarshaller<E> getBytesMarshaller(BytesReader<E> reader) {
        return reader instanceof SimpleBytesReader ? ((SimpleBytesReader) reader).marshaller : null;
    }

    private static class SimpleBytesReader<E> implements BytesReader<E> {
        private static final long serialVersionUID = 0L;

        private final BytesMarshaller<? super E> marshaller;

        public SimpleBytesReader(BytesMarshaller<? super E> marshaller) {
            this.marshaller = marshaller;
        }

        @Override
        public E read(Bytes bytes, long size) {
            return (E) marshaller.read(bytes);
        }

        @Override
        public E read(Bytes bytes, long size, E toReuse) {
            return (E) marshaller.read(bytes, toReuse);
        }
    }

    private BytesReaders() {
    }
}
