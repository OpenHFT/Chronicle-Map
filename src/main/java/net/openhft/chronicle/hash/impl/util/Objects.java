/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.util;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

/**
 * java.util.Objects since Java 7
 */
public final class Objects {
    private Objects() {
    }

    public static int hash(Object... values) {
        return Arrays.hashCode(values);
    }

    public static boolean equal(@Nullable Object a, @Nullable Object b) {
        return a != null ? a.equals(b) : b == null;
    }

    public static boolean builderEquals(@NotNull Object builder, @Nullable Object o) {
        return builder == o ||
                o != null && builder.getClass() == o.getClass() &&
                        builder.toString().equals(o.toString());
    }

    public static void requireNonNull(Object obj) {
        if (obj == null)
            throw new NullPointerException();
    }
}
