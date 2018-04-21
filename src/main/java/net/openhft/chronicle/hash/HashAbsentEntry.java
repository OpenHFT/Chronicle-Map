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

package net.openhft.chronicle.hash;

import net.openhft.chronicle.map.MapAbsentEntry;
import net.openhft.chronicle.set.SetAbsentEntry;
import org.jetbrains.annotations.NotNull;

/**
 * Low-level operational context for the situations, when the new entry is going to be inserted
 * into the {@link ChronicleHash}.
 * <p>
 * <p>This interface is not usable by itself; it merely defines the common base for {@link
 * MapAbsentEntry} and {@link SetAbsentEntry}.
 *
 * @param <K> the hash key type
 * @see HashQueryContext#absentEntry()
 */
public interface HashAbsentEntry<K> {

    /**
     * Returns the context, in which the entry is going to be inserted into the hash.
     */
    HashContext<K> context();

    /**
     * Returns the key is going to be inserted into the {@code ChronicleHash}.
     */
    @NotNull
    Data<K> absentKey();
}
