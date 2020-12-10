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
