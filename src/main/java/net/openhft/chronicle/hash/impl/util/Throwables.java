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

package net.openhft.chronicle.hash.impl.util;

/**
 * Inspired by Guava's Throwables
 */
public final class Throwables {

    private Throwables() {
    }

    public static RuntimeException propagate(Throwable t) {
        // Avoid calling Objects.requireNonNull(), StackOverflowError-sensitive
        if (t == null)
            throw new NullPointerException();
        if (t instanceof Error)
            throw (Error) t;
        if (t instanceof RuntimeException)
            throw (RuntimeException) t;
        throw new RuntimeException(t);
    }

    public static <T extends Throwable> T propagateNotWrapping(
            Throwable t, Class<T> notWrappingThrowableType) throws T {
        Objects.requireNonNull(t);
        Objects.requireNonNull(notWrappingThrowableType);
        if (t instanceof Error)
            throw (Error) t;
        if (t instanceof RuntimeException)
            throw (RuntimeException) t;
        if (notWrappingThrowableType.isInstance(t))
            throw notWrappingThrowableType.cast(t);
        throw new RuntimeException(t);
    }

    public static Throwable returnOrSuppress(Throwable thrown, Throwable t) {
        if (thrown == null) {
            return t;
        } else {
            if (t != null)
                thrown.addSuppressed(t);
            return thrown;
        }
    }
}
