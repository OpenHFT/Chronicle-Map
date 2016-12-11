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
import net.openhft.chronicle.hash.impl.util.Throwables;

import java.util.List;

public final class InMemoryChronicleHashResources extends ChronicleHashResources {

    public InMemoryChronicleHashResources() {}

    @Override
    Throwable releaseSystemResources() {
        Throwable thrown = null;
        List<MemoryResource> memoryResources = memoryResources();
        // Indexed loop instead of for-each because we may be in the context of cleaning up after
        // OutOfMemoryError, and allocating Iterator object may lead to another OutOfMemoryError, so
        // we try to avoid any allocations
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < memoryResources.size(); i++) {
            MemoryResource allocation = memoryResources.get(i);
            try {
                OS.memory().freeMemory(allocation.address, allocation.size);
            } catch (Throwable t) {
                thrown = Throwables.returnOrSuppress(thrown, t);
            }
        }
        return thrown;
    }
}
