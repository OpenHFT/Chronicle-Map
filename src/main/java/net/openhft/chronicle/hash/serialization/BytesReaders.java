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

package net.openhft.chronicle.hash.serialization;

import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshaller;

public final class BytesReaders {

    public static <E> BytesReader<E> fromBytesMarshaller(BytesMarshaller<E> marshaller) {
        return new SimpleBytesReader<E>(marshaller);
    }

    private static class SimpleBytesReader<E> implements BytesReader<E> {
        private static final long serialVersionUID = 0L;

        private final BytesMarshaller<E> marshaller;

        public SimpleBytesReader(BytesMarshaller<E> marshaller) {
            this.marshaller = marshaller;
        }

        @Override
        public E read(Bytes bytes, long size) {
            return marshaller.read(bytes);
        }

        @Override
        public E read(Bytes bytes, long size, E e) {
            return marshaller.read(bytes, e);
        }
    }

    private BytesReaders() {}
}
