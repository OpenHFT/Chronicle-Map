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

package net.openhft.chronicle.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.chronicle.hash.serialization.impl.EnumMarshallable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static java.util.stream.Collectors.toMap;

public class Issue58 {
    File mapFile = new File("string-to-uuid.dat");
    File reverseMapFile = new File("uuid-to-string.dat");

    public static void main(String... args) {
        Map<String, UUID> map = new HashMap<>();
        map.put("AA", UUID.randomUUID());
        map.put("AB", UUID.randomUUID());
        map.put("BA", UUID.randomUUID());
        map.put("BB", UUID.randomUUID());
        try {
            Issue58 test = new Issue58();
            test.writeMapAndReverseMap(map);
            test.test1(map);
            test.test2(map);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void writeMapAndReverseMap(Map<String, UUID> data) throws IOException {
        if (mapFile.exists())
            mapFile.delete();
        if (reverseMapFile.exists())
            reverseMapFile.delete();

        try (
                ChronicleMap<String, UUID> map = ChronicleMap.of(String.class, UUID.class)
                        .averageKeySize(2)
                        .valueMarshaller(UuidMarshaller.INSTANCE)
                        .constantValueSizeBySample(UUID.randomUUID())
                        .entries(data.size())
                        .createPersistedTo(mapFile);
                ChronicleMap<UUID, String> reverseMap = ChronicleMap.of(UUID.class, String.class)
                        .keyMarshaller(UuidMarshaller.INSTANCE)
                        .averageValueSize(2)
                        .constantKeySizeBySample(UUID.randomUUID())
                        .entries(data.size())
                        .createPersistedTo(reverseMapFile)
        ) {

            map.putAll(data);
            Map<UUID, String> reverse = data.keySet().stream()
                    .collect(toMap(k -> data.get(k), k -> k));
            reverseMap.putAll(reverse);
        }
    }

    public void test1(Map<String, UUID> data) throws IOException {
        try (
                ChronicleMap<String, UUID> map = ChronicleMap.of(String.class, UUID.class)
                        .averageKeySize(2)
                        .valueMarshaller(UuidMarshaller.INSTANCE)
                        .constantValueSizeBySample(UUID.randomUUID())
                        .entries(data.size())
                        .createPersistedTo(mapFile);
                ChronicleMap<UUID, String> reverseMap = ChronicleMap.of(UUID.class, String.class)
                        .keyMarshaller(UuidMarshaller.INSTANCE)
                        .constantKeySizeBySample(UUID.randomUUID())
                        .averageValueSize(2)
                        .entries(data.size())
                        .createPersistedTo(reverseMapFile)
        ) {
            System.out.println("first test passes:");
            UUID uuid = data.get("BB");
            System.out.println("BB" + " == " + reverseMap.get(uuid));
            System.out.println(uuid + " == " + map.get("BB"));
        }
    }

    public void test2(Map<String, UUID> data) throws IOException {
        try (
                ChronicleMap<String, UUID> map = ChronicleMap.of(String.class, UUID.class)
                        .averageKeySize(2)
                        .valueMarshaller(UuidMarshaller.INSTANCE)
                        .constantValueSizeBySample(UUID.randomUUID())
                        .entries(data.size())
                        .createPersistedTo(mapFile);
                ChronicleMap<UUID, String> reverseMap = ChronicleMap.of(UUID.class, String.class)
                        .keyMarshaller(UuidMarshaller.INSTANCE)
                        .constantKeySizeBySample(UUID.randomUUID())
                        .averageValueSize(2)
                        .entries(data.size())
                        .createPersistedTo(reverseMapFile)
        ) {
            System.out.println("second identical test also passes:");
            UUID uuid = data.get("BB");
            System.out.println("BB" + " == " + reverseMap.get(uuid));
            System.out.println(uuid + " == " + map.get("BB")); //returns null
            System.out.println("despite all data being present:");
            map.entrySet().forEach(e -> System.out.println(e.getKey() + "->" + e.getValue()));
            reverseMap.forEach((k, v) -> System.out.println(k + " -> " + v));
        }
    }

    static final class UuidMarshaller
            implements BytesReader<UUID>, BytesWriter<UUID>, EnumMarshallable<UuidMarshaller> {
        public static final UuidMarshaller INSTANCE = new UuidMarshaller();

        private UuidMarshaller() {
        }

        @Override
        public void write(Bytes bytes, @NotNull UUID uuid) {
            bytes.writeLong(uuid.getMostSignificantBits());
            bytes.writeLong(uuid.getLeastSignificantBits());
        }

        @NotNull
        @Override
        public UUID read(Bytes bytes, @Nullable UUID using) {
            return new UUID(bytes.readLong(), bytes.readLong());
        }

        @Override
        public UuidMarshaller readResolve() {
            return INSTANCE;
        }
    }
}
