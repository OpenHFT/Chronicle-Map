//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

package net.openhft.chronicle.hash.impl;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.shutdown.PriorityHook;

import java.util.TreeMap;
import java.util.WeakHashMap;

final class ChronicleHashCloseOnExitHook {

    private static WeakHashMap<VanillaChronicleHash<?, ?, ?, ?>.Identity, Long> maps = new WeakHashMap<>();
    private static long order = 0;

    static {
        PriorityHook.add(80, ChronicleHashCloseOnExitHook::closeAll);
    }

    private ChronicleHashCloseOnExitHook() {
    }

    static synchronized void add(VanillaChronicleHash<?, ?, ?, ?> hash) {
        if (maps == null)
            throw new IllegalStateException("Shutdown in progress");
        maps.put(hash.identity, order++);
    }

    static synchronized void remove(VanillaChronicleHash<?, ?, ?, ?> hash) {
        if (maps == null)
            return; // we are already in shutdown
        maps.remove(hash.identity);
    }

    private static void closeAll() {
        try {
            WeakHashMap<VanillaChronicleHash<?, ?, ?, ?>.Identity, Long> maps;
            synchronized (ChronicleHashCloseOnExitHook.class) {
                maps = ChronicleHashCloseOnExitHook.maps;
                ChronicleHashCloseOnExitHook.maps = null;
            }

            TreeMap<Long, VanillaChronicleHash<?, ?, ?, ?>> orderedMaps = new TreeMap<>();
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
                                Jvm.error().on(ChronicleHashCloseOnExitHook.class,
                                        "Error running pre-shutdown action for " + h.toIdentityString() +
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
                        Jvm.error().on(ChronicleHashCloseOnExitHook.class,
                                "Error while closing " + h.toIdentityString() +
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
                Jvm.error().on(ChronicleHashCloseOnExitHook.class,
                        "Error while closing maps during shutdown hook:", throwable);
            } catch (Throwable t2) {
                throwable.addSuppressed(t2);
                throwable.printStackTrace();
            }
        }
    }
}
