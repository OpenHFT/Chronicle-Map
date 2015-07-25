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

package net.openhft.chronicle.hash.serialization.internal;

import net.openhft.chronicle.hash.hashing.LongHashFunction;
import net.openhft.lang.io.Bytes;

public interface MetaBytesInterop<E, I> extends MetaBytesWriter<E, I> {

    boolean startsWith(I interop, Bytes bytes, E e);
    
    <I2> boolean equivalent(I interop, E e,
                            MetaBytesInterop<E, I2> otherMetaInterop, I2 otherInterop, E other);

    long hash(I interop, LongHashFunction hashFunction, E e);
}
