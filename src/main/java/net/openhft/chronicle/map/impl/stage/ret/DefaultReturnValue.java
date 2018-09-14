/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map.impl.stage.ret;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.map.impl.ret.InstanceReturnValue;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;

@Staged
public abstract class DefaultReturnValue<V> implements InstanceReturnValue<V> {
    private V defaultReturnedValue = null;

    abstract boolean defaultReturnedValueInit();

    private void initDefaultReturnedValue(@NotNull Data<V> value) {
        defaultReturnedValue = value.getUsing(null);
    }

    @Override
    public void returnValue(@NotNull Data<V> value) {
        initDefaultReturnedValue(value);
    }

    @Override
    public V returnValue() {
        if (defaultReturnedValueInit()) {
            return defaultReturnedValue;
        } else {
            return null;
        }
    }
}
