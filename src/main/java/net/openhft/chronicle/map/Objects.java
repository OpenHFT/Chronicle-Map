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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

/**
 * java.util.Objects since Java 7
 */
enum Objects {
    ;
    static int hash(Object... values) {
        return Arrays.hashCode(values);
    }

    static boolean equals(@Nullable Object a, @Nullable Object b) {
        return a != null ? a.equals(b) : b == null;
    }

    static boolean builderEquals(@NotNull Object builder, @Nullable Object o) {
        return builder == o ||
                o != null && builder.getClass() == o.getClass() &&
                        builder.toString().equals(o.toString());
    }
}
