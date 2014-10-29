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
import net.openhft.lang.model.Byteable;

/**
 * Marshaller for byte sequences, which are copied to off-heap {@link Bytes}
 * in a very straightforward manner, e. g. {@link Byteable Byteables}, {@code byte[]} arrays,
 * {@code Bytes} themselves. The criterion of this interface applicability --
 * {@link Object#equals(Object)} implementation shouldn't require deserialization and any garbage creation.
 *
 * @param <E> type of marshalled objects
 */
public interface BytesInterop<E> extends BytesWriter<E> {

    boolean startsWith(Bytes bytes, E e);

    long hash(E e);
}
