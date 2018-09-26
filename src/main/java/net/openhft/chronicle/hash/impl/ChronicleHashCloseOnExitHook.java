/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
                    Runnable preShutdownAction = h.getPreShutdownAction();
                    if (preShutdownAction != null) {
                        try {
                            preShutdownAction.run();
                        } catch (Throwable throwable) {
                            try {
                                LOG.error("Error running pre-shutdown action for " + h.toIdentityString() +
                                        " :", throwable);
                            } catch (Throwable t2) {
                                throwable.addSuppressed(t2);
                                throwable.printStackTrace();
                            }
                        }
                    }
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
