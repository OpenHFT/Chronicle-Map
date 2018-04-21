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

import net.openhft.chronicle.map.MapEntry;
import net.openhft.chronicle.set.SetEntry;
import org.jetbrains.annotations.NotNull;

/**
 * A context of a <i>present</i> entry in the {@code ChronicleHash}.
 * <p>
 * <p>This interface is not usable by itself; it merely defines the common base for {@link MapEntry}
 * and {@link SetEntry}.
 *
 * @param <K> type of the key in {@code ChronicleHash}
 * @see HashQueryContext#entry()
 */
public interface HashEntry<K> {
    /**
     * Returns the context, in which the entry is accessed.
     */
    HashContext<K> context();

    /**
     * Returns the entry key.
     */
    @NotNull
    Data<K> key();

    /**
     * Removes the entry from the {@code ChronicleHash}.
     *
     * @throws IllegalStateException if some locking/state conditions required to perform remove
     *                               operation are not met
     */
    void doRemove();
}
