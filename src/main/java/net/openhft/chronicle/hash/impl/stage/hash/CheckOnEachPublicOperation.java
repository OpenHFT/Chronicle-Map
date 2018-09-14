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
