package net.openhft.chronicle.map.internal;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

public final class AnalyticsMain {

    public static final String BASE_MAP_NAME = "analytics_test_map";
    public static final String MAP_NAME1 = BASE_MAP_NAME + "1";
    public static final String MAP_NAME2 = BASE_MAP_NAME + "2";

    public static void main(String[] args) {
        System.out.println("Creating two maps and sending analytics...");
        System.out.println("Currently, there is a limit of four messages per h per JVM instance so only some analytics messages might be sent upstream.");

        try (ChronicleMap<CharSequence, CharSequence> m1 = ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                .name(MAP_NAME1)
                .averageKeySize(32)
                .averageValueSize(32)
                .entries(1_000)
                .create()) {
            try (ChronicleMap<Long, CharSequence> m2 = ChronicleMapBuilder.of(Long.class, CharSequence.class)
                    .name(MAP_NAME2)
                    .averageValueSize(32)
                    .entries(100)
                    .create()) {

                m1.put("A", "1");
                m2.put(2L, "Two");
            }
        }
        Jvm.pause(2_000);
        System.out.println("Completed successfully");
        System.exit(0);
    }

    private static <K, V> ChronicleMap<K, V> createMap(Class<K> keyType, Class<V> valueType, String name) {
        return ChronicleMapBuilder.of(keyType, valueType)
                .name(name)
                .averageKeySize(32)
                .averageValueSize(32)
                .entries(1_000)
                .create();
    }

}