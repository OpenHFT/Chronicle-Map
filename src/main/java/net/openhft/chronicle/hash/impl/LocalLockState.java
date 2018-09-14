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
