/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.onoes.ExceptionKey;
import net.openhft.chronicle.core.onoes.LogLevel;
import net.openhft.chronicle.hash.serialization.impl.SerializableDataAccess;
import net.openhft.chronicle.hash.serialization.impl.SerializableReader;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Regression coverage for Chronicle-Map#462. */
@SuppressWarnings({"rawtypes", "unchecked"})
public class SerializedKeyIdentityTest {

    /**
     * The reporter's mechanism: sorted iteration order is identical, but one object graph contains
     * two equal strings while the other reuses one string. Java serialization records that sharing
     * with a back-reference, so the streams differ despite equal maps and equal iteration order.
     */
    @Test
    public void equalTreeMapsWithDifferentReferenceSharingSerializeDifferently()
            throws IOException {
        final String distinctKey = new String("0");
        final String distinctValue = new String("0");
        final TreeMap<String, String> distinctReferences = new TreeMap<>();
        distinctReferences.put(distinctKey, distinctValue);

        final String shared = new String("0");
        final TreeMap<String, String> sharedReference = new TreeMap<>();
        sharedReference.put(shared, shared);

        assertEquals(distinctReferences, sharedReference);
        assertEquals(distinctReferences.entrySet().toString(),
                sharedReference.entrySet().toString());
        assertNotSame(distinctKey, distinctValue);
        assertSame(sharedReference.firstKey(), sharedReference.get(sharedReference.firstKey()));
        assertFalse(Arrays.equals(javaSerialize(distinctReferences),
                javaSerialize(sharedReference)));
    }

    /** The exact shared-reference mismatch makes an equal TreeMap key unfindable. */
    @Test
    public void equalTreeMapKeyWithDifferentReferenceSharingIsNotFound() {
        final String distinctKey = new String("0");
        final String distinctValue = new String("0");
        final TreeMap<String, String> putKey = new TreeMap<>();
        putKey.put(distinctKey, distinctValue);

        final String shared = new String("0");
        final TreeMap<String, String> lookupKey = new TreeMap<>();
        lookupKey.put(shared, shared);
        assertEquals(putKey, lookupKey);

        try (ChronicleMap<TreeMap, String> map = ChronicleMap
                .of(TreeMap.class, String.class)
                .entries(16)
                .averageKey(putKey)
                .averageValueSize(8)
                .create()) {
            map.put(putKey, "value");
            assertTrue(map.containsKey(putKey));
            assertFalse("an equal key with different serialized bytes is not found",
                    map.containsKey(lookupKey));
        }
    }

    /** In strict mode every fallback Java-serialized key type is rejected, not just containers. */
    @Test
    public void strictModeRejectsNonContainerKeyWhoseEqualsIgnoresAField() {
        assertStrictRejects(ChronicleMap
                .of(Key.class, String.class)
                .entries(16)
                .averageKeySize(64)
                .averageValueSize(8)
                .strictSerializedKeyIdentity(true));
    }

    /** A declared supertype cannot hide a runtime Map from the strict fallback check. */
    @Test
    public void strictModeRejectsDeclaredSupertype() {
        assertStrictRejects(ChronicleMap
                .of(Object.class, String.class)
                .entries(16)
                .averageKeySize(64)
                .averageValueSize(8)
                .strictSerializedKeyIdentity(true));
    }

    /** Explicit serialization is the escape hatch, even if it deliberately uses the same codec. */
    @Test
    public void customDataAccessIsExemptFromStrictFallbackCheck() {
        try (ChronicleMap<Key, String> map = ChronicleMap
                .of(Key.class, String.class)
                .entries(16)
                .averageKeySize(64)
                .averageValueSize(8)
                .keyReaderAndDataAccess(new SerializableReader<>(),
                        new SerializableDataAccess<>())
                .strictSerializedKeyIdentity(true)
                .create()) {
            map.put(new Key(1, "A"), "value");
            assertEquals("value", map.get(new Key(1, "A")));
            assertNull("strict mode trusts, but cannot canonicalize, explicit DataAccess",
                    map.get(new Key(1, "B")));
        }
    }

    /** Default mode records a diagnostic and still permits creation for compatibility. */
    @Test
    public void defaultModeWarnsForFallbackJavaSerialization() {
        final Map<ExceptionKey, Integer> events = Jvm.recordExceptions(false, false, false);
        try {
            try (ChronicleMap<Key, String> ignored = ChronicleMap
                    .of(Key.class, String.class)
                    .entries(16)
                    .averageKeySize(64)
                    .averageValueSize(8)
                    .create()) {
                assertTrue(ignored.isEmpty());
            }
        } finally {
            Jvm.resetExceptionHandlers();
        }

        assertTrue("expected the serialized-key identity warning",
                events.keySet().stream().anyMatch(event ->
                        event.level() == LogLevel.WARN &&
                                event.message().contains("fallback Java serialization") &&
                                event.message().contains(Key.class.getName())));
    }

    /** The system property is evaluated for each new builder and enables the same strict check. */
    @Test
    public void systemPropertyEnablesStrictMode() {
        final String property = "chronicle.map.strictSerializedKeyIdentity";
        final String previous = System.getProperty(property);
        System.setProperty(property, "true");
        try {
            assertStrictRejects(ChronicleMap
                    .of(Key.class, String.class)
                    .entries(16)
                    .averageKeySize(64)
                    .averageValueSize(8));
        } finally {
            if (previous == null)
                System.clearProperty(property);
            else
                System.setProperty(property, previous);
        }
    }

    /** The insertion-order example remains useful as an additional, distinct failure mechanism. */
    @Test
    public void equalLinkedHashMapsCanSerializeToDifferentBytes() throws IOException {
        final LinkedHashMap<String, String> first = new LinkedHashMap<>();
        first.put("x", "1");
        first.put("y", "2");
        final LinkedHashMap<String, String> second = new LinkedHashMap<>();
        second.put("y", "2");
        second.put("x", "1");

        assertEquals(first, second);
        assertFalse(Arrays.equals(javaSerialize(first), javaSerialize(second)));
    }

    private static void assertStrictRejects(ChronicleMapBuilder<?, String> builder) {
        try {
            builder.create().close();
            fail("expected strict mode to reject fallback Java serialization");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("serialized bytes"));
        }
    }

    private static byte[] javaSerialize(Serializable value) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(value);
        }
        return bytes.toByteArray();
    }

    private static final class Key implements Serializable {
        private static final long serialVersionUID = 0L;

        private final int id;
        private final String label;

        private Key(int id, String label) {
            this.id = id;
            this.label = label;
        }

        @Override
        public boolean equals(Object object) {
            return object instanceof Key && id == ((Key) object).id;
        }

        @Override
        public int hashCode() {
            return id;
        }
    }
}
