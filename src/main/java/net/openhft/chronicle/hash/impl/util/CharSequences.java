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
