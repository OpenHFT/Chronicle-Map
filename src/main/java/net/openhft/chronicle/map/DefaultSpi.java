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

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.map.replication.MapRemoteOperations;
import org.jetbrains.annotations.NotNull;

final class DefaultSpi implements MapMethods, MapEntryOperations, MapRemoteOperations,
        DefaultValueProvider {
    static final DefaultSpi DEFAULT_SPI = new DefaultSpi();

    static <K, V, R> MapMethods<K, V, R> mapMethods() {
        return DEFAULT_SPI;
    }

    static <K, V, R> MapEntryOperations<K, V, R> mapEntryOperations() {
        return DEFAULT_SPI;
    }

    static <K, V, R> MapRemoteOperations<K, V, R> mapRemoteOperations() {
        return DEFAULT_SPI;
    }

    static <K, V> DefaultValueProvider<K, V> defaultValueProvider() {
        return DEFAULT_SPI;
    }

    @Override
    public Data defaultValue(@NotNull MapAbsentEntry absentEntry) {
        return absentEntry.defaultValue();
    }
}
