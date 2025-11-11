/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.util.Time;

import java.io.File;
import java.nio.file.Paths;

public class Issue62ChronicleServer {

    final static String STR =
            //"This is just a long string, which causes sink to fail for some reason.";
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    final static File MAP_FILE_B =
            Paths.get(OS.getTarget(), Time.uniqueId() + "map.b").toFile();
    private final static File MAP_FILE_A =
            Paths.get(OS.getTarget(), Time.uniqueId() + "map.a").toFile();

    static void prepare(File file) {
        if (file.exists())
            file.delete();
        file.deleteOnExit();
    }

    public static void main(String[] args) throws Exception {
/*
        prepare(MAP_FILE_A);

        ChronicleMapBuilder<String, Long> cityPostalCodesMapBuilder =
                ChronicleMapBuilder.of(String.class, Long.class)
                        //.averageKeySize(100)
                        .averageKey(STR)
                        .entries(50_000);

        ((ChronicleHashBuilderPrivateAPI<?, ?>) cityPostalCodesMapBuilder.privateAPI())
                .replication((byte) 1);

        ChronicleMap<String, Long> cityPostalCodes =
                cityPostalCodesMapBuilder.createPersistedTo(MAP_FILE_A);

        for (int i = 0; i < 100; i++) {
            cityPostalCodes.put(STR + i, (long) i);
        }

        System.out.println("Map created");
        Jvm.pause(15000);
*/
    }
}
