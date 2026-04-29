/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

public class Issue62ChronicleClient {

    public static void main(String[] args) {
        /*prepare(MAP_FILE_B);

        ChronicleMapBuilder<String, Long> cityPostalCodesMapBuilder =
                ChronicleMapBuilder.of(String.class, Long.class)
                        //.averageKeySize(100)
                        .averageKey(Issue62ChronicleServer.STR)
                        .entries(50_000);

        ((ChronicleHashBuilderPrivateAPI<?, ?>) cityPostalCodesMapBuilder.privateAPI())
                .replication((byte) 2);

        try (ChronicleMap<String, Long> map =
                     cityPostalCodesMapBuilder.createPersistedTo(MAP_FILE_B)) {

            System.out.println( "Starting");
            Jvm.pause(3000);

            for (Entry<String, Long> entry : map.entrySet()) {
                System.out.println(entry);
            }
        }*/
    }
}
