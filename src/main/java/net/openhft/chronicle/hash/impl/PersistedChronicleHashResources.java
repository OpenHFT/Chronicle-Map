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
