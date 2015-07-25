/*
 *      Copyright (C) 2015  higherfrequencytrading.com
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.hash.impl;

import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.hash.AbstractData;
import net.openhft.lang.io.DirectBytes;
import net.openhft.lang.io.DirectStore;
import net.openhft.lang.io.serialization.JDKObjectSerializer;

public abstract class CopyingInstanceData<I> extends AbstractData<I> {

    private final JavaLangBytesReusableBytesStore bytesStore =
            new JavaLangBytesReusableBytesStore();

    public DirectBytes getBuffer(DirectBytes b, long capacity) {
        if (b != null) {
            if (b.capacity() >= capacity) {
                return (DirectBytes) b.clear();
            } else {
                DirectStore store = (DirectStore) b.store();
                store.resize(capacity, false);
                DirectBytes bytes = store.bytes();
                bytesStore.setBytes(bytes);
                return bytes;
            }
        } else {
            DirectBytes bytes = new DirectStore(JDKObjectSerializer.INSTANCE,
                    Math.max(1, capacity), true).bytes();
            bytesStore.setBytes(bytes);
            return bytes;
        }
    }

    @Override
    public RandomDataInput bytes() {
        return bytesStore;
    }

    public abstract I instance();

    public abstract DirectBytes buffer();

    @Override
    public long offset() {
        return 0;
    }

    @Override
    public long size() {
        return buffer().limit();
    }

    @Override
    public I get() {
        return instance();
    }
}
