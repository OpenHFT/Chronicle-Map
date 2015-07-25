/*
 *      Copyright (C) 2015  higherfrequencytrading.com
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

package net.openhft.chronicle.hash.impl;

public enum LocalLockState {
    UNLOCKED(false, false, false),
    READ_LOCKED(true, false, false),
    UPDATE_LOCKED(true, true, false),
    WRITE_LOCKED(true, true, true);

    public final boolean read;
    public final boolean update;
    public final boolean write;

    LocalLockState(boolean read, boolean update, boolean write) {
        this.read = read;
        this.update = update;
        this.write = write;
    }
}
