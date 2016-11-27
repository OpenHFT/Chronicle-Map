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

import net.openhft.chronicle.hash.impl.util.Throwables;

import java.util.ArrayList;
import java.util.List;

/**
 * ChronicleHashResources is Runnable to be passed as "hunk" to {@link sun.misc.Cleaner}.
 */
public abstract class ChronicleHashResources implements Runnable {

    List<MemoryResource> memoryResources = new ArrayList<>(1);

    public final synchronized void addMemoryResource(long address, long size) {
        if (memoryResources == null)
            throw new IllegalStateException("Already released");
        memoryResources.add(new MemoryResource(address, size));
    }

    public final synchronized long totalMemory() {
        if (memoryResources == null)
            return 0L;
        long totalMemory = 0L;
        for (MemoryResource memoryResource : memoryResources) {
            totalMemory += memoryResource.size;
        }
        return totalMemory;
    }

    abstract Throwable doRelease();

    public final synchronized void releaseManually() {
        if (memoryResources == null)
            return; // Already released
        Throwable thrown = null;
        try {
            thrown = doRelease();
        } finally {
            memoryResources = null;
        }
        if (thrown != null)
            throw Throwables.propagate(thrown);
    }
}
