/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.util;

import org.jetbrains.annotations.NotNull;

public final class CharSequences {

    private CharSequences() {
    }

    public static boolean equivalent(@NotNull CharSequence a, @NotNull CharSequence b) {
        if (a.equals(b))
            return true;
        if (a instanceof String)
            return ((String) a).contentEquals(b);
        if (b instanceof String)
            return ((String) b).contentEquals(a);
        int len = a.length();
        if (len != b.length())
            return false;
        for (int i = 0; i < len; i++) {
            if (a.charAt(i) != b.charAt(i))
                return false;
        }
        return true;
    }

    public static int hash(@NotNull CharSequence cs) {
        if (cs instanceof String)
            return cs.hashCode();
        int h = 0;
        for (int i = 0, len = cs.length(); i < len; i++) {
            h = 31 * h + cs.charAt(i);
        }
        return h;
    }
}
