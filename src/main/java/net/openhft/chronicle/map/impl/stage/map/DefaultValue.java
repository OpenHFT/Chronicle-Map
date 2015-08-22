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

package net.openhft.chronicle.map.impl.stage.map;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.chronicle.map.MapAbsentEntry;
import net.openhft.chronicle.map.impl.VanillaChronicleMapHolder;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;

@Staged
public abstract class DefaultValue<K, V> implements MapAbsentEntry<K, V> {

    @StageRef CheckOnEachPublicOperation checkOnEachPublicOperation;
    @StageRef VanillaChronicleMapHolder<K, ?, ?, V, ?, ?, ?> mh;

    @NotNull
    @Override
    public Data<V> defaultValue() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        if (mh.m().constantValueProvider == null) {
            throw new IllegalStateException("to call acquireUsing(), " +
                    "or defaultValue() on AbsentEntry, you should configure " +
                    "ChronicleMapBuilder.defaultValue(), or use one of the 'known' value types: " +
                    "boxed primitives, or so-called data-value-generated interface as a value");
        }
        return context().wrapValueAsData(mh.m().constantValueProvider.defaultValue());
    }
}
