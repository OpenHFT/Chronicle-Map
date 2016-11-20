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
 * Thrown when a {@link ChronicleHash} is accessed after {@link ChronicleHash#close()}.
 */
public final class ChronicleHashClosedException extends IllegalStateException {
    private static final long serialVersionUID = 0L;

    public ChronicleHashClosedException(ChronicleHash hash) {
        super("Access to " + hash.toIdentityString() + " after close()");
    }
}
