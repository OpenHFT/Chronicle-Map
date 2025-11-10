//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

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
