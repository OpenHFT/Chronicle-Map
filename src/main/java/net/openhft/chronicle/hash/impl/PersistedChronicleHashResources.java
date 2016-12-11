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
import net.openhft.chronicle.hash.impl.util.Throwables;

import java.io.File;
import java.util.List;


public final class PersistedChronicleHashResources extends ChronicleHashResources {

    private final File file;

    public PersistedChronicleHashResources(File file) {
        this.file = file;
    }

    @Override
    Throwable releaseSystemResources() {
        Throwable thrown = null;
        // Paranoiac mode: releaseMappedMemory() should never throw any Throwable (it should return
        // it), but we don't trust ourselves and call releaseFile() in finally block.
        try {
            thrown = releaseMappedMemory();
        } finally {
            thrown = releaseFile(thrown);
        }
        return thrown;
    }

    private Throwable releaseMappedMemory() {
        Throwable thrown = null;
        List<MemoryResource> memoryResources = memoryResources();
        // Indexed loop instead of for-each because we may be in the context of cleaning up after
        // OutOfMemoryError, and allocating Iterator object may lead to another OutOfMemoryError, so
        // we try to avoid any allocations
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < memoryResources.size(); i++) {
            MemoryResource mapping = memoryResources.get(i);
            try {
                OS.unmap(mapping.address, mapping.size);
            } catch (Throwable t) {
                thrown = Throwables.returnOrSuppress(thrown, t);
            }
        }
        return thrown;
    }

    private Throwable releaseFile(Throwable thrown) {
        try {
            CanonicalRandomAccessFiles.release(file);
        } catch (Throwable t) {
            thrown = Throwables.returnOrSuppress(thrown, t);
        }
        return thrown;
    }
}
