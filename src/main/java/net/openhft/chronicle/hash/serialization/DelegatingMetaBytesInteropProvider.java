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

import net.openhft.lang.threadlocal.ThreadLocalCopies;

import java.io.ObjectStreamException;

public final class DelegatingMetaBytesInteropProvider<E, I extends BytesInterop<E>>
        implements MetaProvider<E, I, MetaBytesInterop<E, I>> {
    private static final long serialVersionUID = 0L;
    private static final DelegatingMetaBytesInteropProvider INSTANCE =
            new DelegatingMetaBytesInteropProvider();

    public static <E, I extends BytesInterop<E>>
    MetaProvider<E, I, MetaBytesInterop<E, I>> instance() {
        return INSTANCE;
    }

    private DelegatingMetaBytesInteropProvider() {}

    @Override
    public MetaBytesInterop<E, I> get(ThreadLocalCopies copies,
                                      MetaBytesInterop<E, I> originalMetaWriter, I writer, E e) {
        return originalMetaWriter;
    }

    @Override
    public ThreadLocalCopies getCopies(ThreadLocalCopies copies) {
        return copies;
    }

    private Object readResolve() throws ObjectStreamException {
        return INSTANCE;
    }
}
