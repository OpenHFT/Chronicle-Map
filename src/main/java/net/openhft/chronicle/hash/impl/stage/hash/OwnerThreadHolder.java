//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

package net.openhft.chronicle.hash.impl.stage.hash;

import net.openhft.chronicle.hash.impl.VanillaChronicleHashHolder;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

import java.util.ConcurrentModificationException;

@Staged
public class OwnerThreadHolder {

    final Thread owner = Thread.currentThread();
    @StageRef
    VanillaChronicleHashHolder<?> hh;

    public void checkAccessingFromOwnerThread() {
        if (owner != Thread.currentThread()) {
            throw new ConcurrentModificationException(hh.h().toIdentityString() +
                    ": Context shouldn't be accessed from multiple threads");
        }
    }
}
