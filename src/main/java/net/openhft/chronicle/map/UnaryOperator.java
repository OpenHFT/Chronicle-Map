/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.map;

import java.io.Serializable;

/**
 * Represents a mutator that accepts one mutable argument, which it may alter and produces a result.
 * <p>
 * <p>This is not functional as it can alter an argument.
 *
 * @param <T> the type of the mutable input
 */

public interface UnaryOperator<T> extends Serializable {
    /**
     * Applies this mutator to the given mutable argument.
     *
     * @param t the mutator argument
     * @return the  result
     */
    T update(T t);
}
