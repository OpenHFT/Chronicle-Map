//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

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
