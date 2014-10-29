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

import java.io.ObjectStreamException;

public final class DelegatingMetaBytesInterop<E, I extends BytesInterop<E>>
        implements MetaBytesInterop<E, I> {
    private static final long serialVersionUID = 0L;
    private static final DelegatingMetaBytesInterop INSTANCE = new DelegatingMetaBytesInterop();

    public static <E, I extends BytesInterop<E>>
    DelegatingMetaBytesInterop<E, I> instance() {
        return INSTANCE;
    }

    private DelegatingMetaBytesInterop() {}

    @Override
    public long size(I interop, E e) {
        return interop.size(e);
    }

    @Override
    public boolean startsWith(I interop, Bytes bytes, E e) {
        return interop.startsWith(bytes, e);
    }

    @Override
    public long hash(I interop, E e) {
        return interop.hash(e);
    }

    @Override
    public void write(I interop, Bytes bytes, E e) {
        interop.write(bytes, e);
    }

    private Object readResolve() throws ObjectStreamException {
        return INSTANCE;
    }
}
