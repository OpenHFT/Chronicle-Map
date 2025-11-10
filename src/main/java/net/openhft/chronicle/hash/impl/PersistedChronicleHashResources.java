//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

package net.openhft.chronicle.hash.impl;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.hash.impl.util.CanonicalRandomAccessFiles;

import java.io.File;
import java.io.IOException;

public final class PersistedChronicleHashResources extends ChronicleHashResources {

    private File file;

    public PersistedChronicleHashResources(File file) {
        this.file = file;
        OS.memory().storeFence(); // Emulate final semantics of the file field
    }

    @Override
    void releaseMemoryResource(MemoryResource mapping) throws IOException {
        OS.unmap(mapping.address, mapping.size);
    }

    @Override
    Throwable releaseExtraSystemResources() {
        if (file == null)
            return null;
        Throwable thrown = null;
        try {
            CanonicalRandomAccessFiles.release(file);
            file = null;
        } catch (Throwable t) {
            thrown = t;
        }
        return thrown;
    }
}
