/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
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

package net.openhft.chronicle.map.impl;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.map.impl.ret.InstanceReturnValue;
import org.jetbrains.annotations.NotNull;

public final class NullReturnValue implements InstanceReturnValue {
    private static final NullReturnValue NULL_RETURN_VALUE = new NullReturnValue();

    private NullReturnValue() {
    }

    public static <V> InstanceReturnValue<V> get() {
        return NULL_RETURN_VALUE;
    }

    @Override
    public Object returnValue() {
        return null;
    }

    @Override
    public void returnValue(@NotNull Data value) {
        // ignore
    }
}
