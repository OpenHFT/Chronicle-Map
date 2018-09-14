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
