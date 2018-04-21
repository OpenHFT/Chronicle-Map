/*
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TreeMap;
import java.util.WeakHashMap;

final class ChronicleHashCloseOnExitHook {
    private static final Logger LOG = LoggerFactory.getLogger(ChronicleHashCloseOnExitHook.class);

    private static WeakHashMap<VanillaChronicleHash.Identity, Long> maps = new WeakHashMap<>();
    private static long order = 0;

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(ChronicleHashCloseOnExitHook::closeAll));
    }

    private ChronicleHashCloseOnExitHook() {
    }

    static synchronized void add(VanillaChronicleHash hash) {
        if (maps == null)
            throw new IllegalStateException("Shutdown in progress");
        maps.put(hash.identity, order++);
    }

    static synchronized void remove(VanillaChronicleHash hash) {
        if (maps == null)
            return; // we are already in shutdown
        maps.remove(hash.identity);
    }

    private static void closeAll() {
        try {
            WeakHashMap<VanillaChronicleHash.Identity, Long> maps;
            synchronized (ChronicleHashCloseOnExitHook.class) {
                maps = ChronicleHashCloseOnExitHook.maps;
                ChronicleHashCloseOnExitHook.maps = null;
            }

            TreeMap<Long, VanillaChronicleHash> orderedMaps = new TreeMap<>();
            maps.forEach((identity, order) -> orderedMaps.put(order, identity.hash()));
            // close later added maps first
            orderedMaps.descendingMap().values().forEach(h -> {
                try {
                    h.close();
                } catch (Throwable throwable) {
                    try {
                        LOG.error("Error while closing " + h.toIdentityString() +
                                " during shutdown hook:", throwable);
                    } catch (Throwable t2) {
                        // This may occur if the log service has already been shut down. Try to fall
                        // back to printStackTrace().
                        throwable.addSuppressed(t2);
                        throwable.printStackTrace();
                    }
                }
            });
        } catch (Throwable throwable) {
            try {
                LOG.error("Error while closing maps during shutdown hook:", throwable);
            } catch (Throwable t2) {
                throwable.addSuppressed(t2);
                throwable.printStackTrace();
            }
        }
    }
}
