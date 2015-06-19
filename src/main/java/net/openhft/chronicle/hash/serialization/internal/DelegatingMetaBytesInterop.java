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

import net.openhft.chronicle.hash.serialization.BytesInterop;
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
