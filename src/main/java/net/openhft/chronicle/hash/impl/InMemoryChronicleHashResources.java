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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class InMemoryChronicleHashResources extends ChronicleHashResources {

    private static final Logger LOG =
            LoggerFactory.getLogger(PersistedChronicleHashResources.class);

    public InMemoryChronicleHashResources() {}

    @Override
    public synchronized void run() {
        if (memoryResources == null)
            return; // Already released
        // All Throwables are caught because this class is used as Runnable for sun.misc.Cleaner,
        // hence must not fail
        try {
            Throwable thrown = doRelease();
            if (thrown != null) {
                try {
                    LOG.error("Error on releasing memory of a Chronicle Map:", thrown);
                } catch (Throwable t) {
                    // This may occur if Releaser is run in a shutdown hook, and the log service has
                    // already been shut down. Try to fall back to printStackTrace().
                    thrown.addSuppressed(t);
                    thrown.printStackTrace();
                }
            }
        } finally {
            memoryResources = null;
        }
    }

    @Override
    Throwable doRelease() {
        Throwable thrown = null;
        for (MemoryResource allocation : memoryResources) {
            try {
                OS.memory().freeMemory(allocation.address, allocation.size);
            } catch (Throwable t) {
                if (thrown == null) {
                    thrown = t;
                } else {
                    thrown.addSuppressed(t);
                }
            }
        }
        return thrown;
    }
}
