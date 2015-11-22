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

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.SizedWriter;

import static net.openhft.chronicle.hash.serialization.StatefulCopyable.copyIfNeeded;

public class NotReusingSizedMarshallableDataAccess<T> extends SizedMarshallableDataAccess<T> {
    public NotReusingSizedMarshallableDataAccess(
            Class<T> tClass, SizedReader<T> sizedReader, SizedWriter<? super T> sizedWriter) {
        super(tClass, sizedReader, sizedWriter);
    }

    @Override
    protected T createInstance() {
        return null;
    }

    @Override
    public DataAccess<T> copy() {
        return new NotReusingSizedMarshallableDataAccess<>(
                tClass, copyIfNeeded(sizedReader), copyIfNeeded(sizedWriter));
    }
}
