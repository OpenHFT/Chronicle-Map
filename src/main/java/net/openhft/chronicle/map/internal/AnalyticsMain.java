package net.openhft.chronicle.map.internal;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

public final class AnalyticsMain {

    public static final String BASE_MAP_NAME = "analytics_test_map";
    public static final String MAP_NAME1 = BASE_MAP_NAME + "1";
    public static final String MAP_NAME2 = BASE_MAP_NAME + "2";

    public static void main(String[] args) {
        System.out.println("Creating two maps and sending analytics...");
        System.out.println("Currently, there is a limit of one message per h per JVM instance so only one analytics message will be sent upstream.");

        try (ChronicleMap<CharSequence, CharSequence> m1 = createMap(CharSequence.class, CharSequence.class, MAP_NAME1)) {
            try (ChronicleMap<Long, CharSequence> m2 = createMap(Long.class, CharSequence.class, MAP_NAME2)) {
                m1.put("A", "1");
                m2.put(2L, "Two");
            }
        }
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