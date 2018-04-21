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

package net.openhft.chronicle.map.impl.stage.ret;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.map.impl.ret.UsableReturnValue;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;

@Staged
public abstract class UsingReturnValue<V> implements UsableReturnValue<V> {

    private V usingReturnValue = (V) USING_RETURN_VALUE_UNINIT;
    private V returnedValue = null;

    @Override
    public void initUsingReturnValue(V usingReturnValue) {
        this.usingReturnValue = usingReturnValue;
    }

    abstract boolean returnedValueInit();

    private void initReturnedValue(@NotNull Data<V> value) {
        returnedValue = value.getUsing(usingReturnValue);
    }

    @Override
    public void returnValue(@NotNull Data<V> value) {
        initReturnedValue(value);
    }

    @Override
    public V returnValue() {
        if (returnedValueInit()) {
            return returnedValue;
        } else {
            return null;
        }
    }
}
