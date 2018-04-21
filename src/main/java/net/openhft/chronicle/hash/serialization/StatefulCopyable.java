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

package net.openhft.chronicle.hash.serialization;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.set.ChronicleSet;

/**
 * {@link Cloneable}-like interface used by Chronicle Map to populate stateful serializer
 * implementations for each isolated site (thread + Chronicle Map instance + objects domain).
 * Stateful implementations of serialization interfaces ({@link SizedReader}, {@link SizedWriter},
 * {@link BytesReader}, {@link BytesWriter} or {@link DataAccess}), configured for {@link
 * ChronicleMap} or {@link ChronicleSet} in builder, should implement the {@code StatefulCopyable}
 * interface.
 * <p>
 * <p>See <a href="https://github.com/OpenHFT/Chronicle-Map#understanding-statefulcopyable">
 * Understanding {@code StatefulCopyable}</a> section in the Chronicle Map tutorial for more info
 * on how to implement and use this interface properly, and for examples.
 *
 * @param <T> the type of marshaller, implementing this interface
 */
public interface StatefulCopyable<T extends StatefulCopyable<T>> {

    /**
     * Checks if {@code possiblyStatefulCopyable} implements {@code StatefulCopyable}, then returns
     * {@link #copy()} of it, otherwise returns the {@code possiblyStatefulCopyable} itself.
     *
     * @param possiblyStatefulCopyable the instance to {@code copy()} if it implements {@code
     *                                 StatefulCopyable}, or to return without modification
     * @param <T>                      the type of the passed instance
     * @return the copy of the passed instance with independent state, of the instance back, if it
     * doesn't implement {@code StatefulCopyable}
     */
    static <T> T copyIfNeeded(T possiblyStatefulCopyable) {
        if (possiblyStatefulCopyable instanceof StatefulCopyable) {
            //noinspection unchecked
            return (T) ((StatefulCopyable) possiblyStatefulCopyable).copy();
        }
        return possiblyStatefulCopyable;
    }

    /**
     * Creates a copy of this marshaller, with independent state. The current state itself shouldn't
     * be copied (it could be "clear" in the copy), only "configuration" of the instance, on which
     * {@code copy()} is called, should be inherited in the copy (e. g. the class of objects
     * serialized). So, {@code copy()} should be transitive, i. e. {@code marshaller.copy()} and
     * {@code marshaller.copy().copy()} should result to identical instances.
     * <p>
     * <p>The state of the instance on which {@code copy()} is called shouldn't be changed.
     * <p>
     * <p>If some marshaller is ought to implement {@code StatefulCopyable} interface (e. g.
     * {@link DataAccess}) but is stateless actually, it could return {@code this} from this method.
     *
     * @return the copy if this marshaller with the same configuration, but independent state
     */
    T copy();
}
