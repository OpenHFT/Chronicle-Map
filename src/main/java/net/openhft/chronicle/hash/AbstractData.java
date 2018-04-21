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

/**
 * Defines reasonable defaults for {@code Data}'s {@code equals()}, {@code hashCode()} and
 * {@code toString()}. They should be default implementations in the {@code Data} interface itself,
 * but Java 8 doesn't allow to override {@code Object}'s methods by default implementations
 * in interfaces.
 */
public abstract class AbstractData<T> implements Data<T> {

    /**
     * Constructor for use by subclasses.
     */
    protected AbstractData() {
    }

    /**
     * Computes value's hash code by applying a hash function to {@code Data}'s <i>bytes</i>
     * representation.
     *
     * @implNote delegates to {@link #dataHashCode()}.
     */
    @Override
    public int hashCode() {
        return dataHashCode();
    }

    /**
     * Compares {@code Data}s' <i>bytes</i> representations.
     *
     * @implNote delegates to {@link #dataEquals(Object)}.
     */
    @Override
    public boolean equals(Object obj) {
        return dataEquals(obj);
    }

    /**
     * Delegates to {@code Data}'s <i>object</i> {@code toString()}. If deserialization fails with
     * exception (e. g. if data bytes are corrupted, and represent not a valid serialized form of
     * an object), traces the data's bytes and the exception.
     *
     * @implNote delegates to {@link #dataToString()}.
     */
    @Override
    public String toString() {
        return dataToString();
    }
}
