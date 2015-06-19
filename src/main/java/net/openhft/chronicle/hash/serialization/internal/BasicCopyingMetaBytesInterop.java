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

import net.openhft.chronicle.hash.hashing.Hasher;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.threadlocal.Provider;
import net.openhft.lang.threadlocal.ThreadLocalCopies;

public abstract class BasicCopyingMetaBytesInterop<E, W> implements MetaBytesInterop<E, W> {
    private static final long serialVersionUID = 0L;

    static final Provider<DirectBytesBuffer> provider =
            Provider.of(DirectBytesBuffer.class);

    static abstract class BasicCopyingMetaBytesInteropProvider<E, I,
            MI extends MetaBytesInterop<E, I>> implements MetaProvider<E, I, MI> {
        private static final long serialVersionUID = 0L;

        @Override
        public ThreadLocalCopies getCopies(ThreadLocalCopies copies) {
            return provider.getCopies(copies);
        }
    }

    final DirectBytesBuffer buffer;
    transient long size;
    transient long hash;

    protected BasicCopyingMetaBytesInterop(DirectBytesBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public long size(W writer, E e) {
        return size;
    }

    @Override
    public boolean startsWith(W writer, Bytes bytes, E e) {
        return bytes.startsWith(buffer.buffer);
    }

    @Override
    public long hash(W writer, E e) {
        long h;
        if ((h = hash) == 0L)
            return hash = Hasher.hash(buffer.buffer);
        return h;
    }

    @Override
    public void write(W writer, Bytes bytes, E e) {
        bytes.write(buffer.buffer, buffer.buffer.position(), buffer.buffer.remaining());
    }
}
