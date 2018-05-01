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

package net.openhft.chronicle.hash.impl.stage.hash;

import net.openhft.chronicle.hash.impl.stage.entry.ReadLock;
import net.openhft.chronicle.hash.impl.stage.entry.UpdateLock;
import net.openhft.chronicle.hash.impl.stage.entry.WriteLock;
import net.openhft.chronicle.map.impl.stage.query.QueryCheckOnEachPublicOperation;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public abstract class CheckOnEachPublicOperation {

    @StageRef
    OwnerThreadHolder holder;

    public void checkOnEachPublicOperation() {
        checkOnEachLockOperation();
    }

    /**
     * This method call prefix methods in {@link ReadLock}, {@link UpdateLock}, {@link WriteLock}.
     * Distinction from {@link #checkOnEachPublicOperation()} is needed because {@link
     * QueryCheckOnEachPublicOperation#checkOnEachPublicOperation()} depends on some stages that
     * depend on locking methods, closing a dependency cycle.
     */
    public void checkOnEachLockOperation() {
        holder.checkAccessingFromOwnerThread();
    }
}
