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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;


public final class PersistedChronicleHashResourceReleaser
        extends ChronicleHashResourceReleaser {
    private static final Logger LOG =
            LoggerFactory.getLogger(PersistedChronicleHashResourceReleaser.class);

    private final File file;

    public PersistedChronicleHashResourceReleaser(File file) {
        this.file = file;
    }

    @Override
    public synchronized void run() {
        if (memoryResources == null)
            return; // Already released
        // All Throwables are catched because this class is used as Runnable for sun.misc.Cleaner,
        // hence must not fail
        Throwable thrown = null;
        try {
            thrown = doRelease();
        } finally {
            memoryResources = null;
        }
        if (thrown != null) {
            try {
                LOG.error("Error on releasing memory and RAF of a Chronicle Map at file=" + file,
                        thrown);
            } catch (Throwable t) {
                // This may occur if Releaser is run in a shutdown hook, and the log service has
                // already been shut down. Try to fall back to printStackTrace().
                thrown.addSuppressed(t);
                thrown.printStackTrace();
            }
        }
    }

    @Override
    Throwable doRelease() {
        Throwable thrown = null;
        for (MemoryResource mapping : memoryResources) {
            try {
                OS.unmap(mapping.address, mapping.size);
            } catch (Throwable t) {
                if (thrown == null) {
                    thrown = t;
                } else {
                    thrown.addSuppressed(t);
                }
            }
        }
        try {
            CanonicalRandomAccessFiles.release(file);
        } catch (Throwable t) {
            if (thrown == null) {
                thrown = t;
            } else {
                thrown.addSuppressed(t);
            }
        }
        return thrown;
    }
}
