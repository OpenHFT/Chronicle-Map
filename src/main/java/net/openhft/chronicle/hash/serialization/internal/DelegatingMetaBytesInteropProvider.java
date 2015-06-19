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
