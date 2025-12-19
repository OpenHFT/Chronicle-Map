/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class NestedContextsInIterationContextTest {

    @Test
    public void testNestedGetsInIterationContextAllowed() {
        try (ChronicleMap<Integer, Integer> map = ChronicleMapBuilder
                .of(Integer.class, Integer.class)
                .entries(100)
                .create()) {
            map.put(42, 42);
            int[] visited = new int[1];
            Integer[] nestedGet = new Integer[1];
            map.forEachEntry(e -> {
                visited[0]++;
                nestedGet[0] = map.get(0);
            });
            assertEquals(1, visited[0], "expected one entry visited");
            assertNull(nestedGet[0], "nested get should return null for absent key");
        }
    }

    @Test
    public void testNestedPutsInIterationContextForbidden() {
        assertThrows(IllegalStateException.class, () -> {
            try (ChronicleMap<Integer, Integer> map = ChronicleMapBuilder
                    .of(Integer.class, Integer.class)
                    .entries(100)
                    .create()) {
                map.put(42, 42);
                map.forEachEntry(e -> map.put(0, 0));
            }
        });
    }

    @Test
    public void testNestedUpdatesDifferentSegmentInIterationContextForbidden() {
        boolean[] sawExpected = new boolean[1];
        try (ChronicleMap<Integer, Integer> map = ChronicleMapBuilder
                .of(Integer.class, Integer.class)
                .entries(100)
                .actualSegments(2)
                .create()) {
            map.put(42, 42);
            ExternalMapQueryContext<Integer, Integer, ?> queryCxt = map.queryContext(42);
            int keySegmentIndex = queryCxt.segmentIndex();
            try (MapSegmentContext<Integer, Integer, ?> cxt = map.segmentContext(keySegmentIndex)) {
                cxt.forEachSegmentEntry(e -> {
                    int k = 0;
                    ExternalMapQueryContext<Integer, Integer, ?> nestedCxt;
                    // try lock different segment
                    while ((nestedCxt = map.queryContext(k)).segmentIndex() == keySegmentIndex) {
                        k++;
                    }
                    try (ExternalMapQueryContext<Integer, Integer, ?> nestedC = nestedCxt) {
                        nestedC.updateLock().lock();
                    } catch (IllegalStateException ex) {
                        // expected

                        // try lock the same segment
                        while ((nestedCxt = map.queryContext(k)).segmentIndex() !=
                                keySegmentIndex) {
                            k++;
                        }
                        try (ExternalMapQueryContext<Integer, Integer, ?> nestedC2 = nestedCxt) {
                            nestedC2.updateLock().lock();
                        } catch (IllegalStateException ex2) {
                            // expected
                            sawExpected[0] = true;
                            return;
                        }
                    }
                    throw new AssertionError("expected " + IllegalStateException.class);
                });
            } finally {
                queryCxt.close();
            }
        }
        assertTrue(sawExpected[0], "expected IllegalStateException for nested update locks");
    }

    @Test
    public void testNestedWritesDifferentSegmentInIterationContextForbidden() {
        boolean[] sawExpected = new boolean[1];
        try (ChronicleMap<Integer, Integer> map = ChronicleMapBuilder
                .of(Integer.class, Integer.class)
                .entries(100)
                .actualSegments(2)
                .create()) {
            map.put(42, 42);
            ExternalMapQueryContext<Integer, Integer, ?> queryCxt = map.queryContext(42);
            int keySegmentIndex = queryCxt.segmentIndex();
            try (MapSegmentContext<Integer, Integer, ?> cxt = map.segmentContext(keySegmentIndex)) {
                cxt.forEachSegmentEntry(e -> {
                    int k = 0;
                    ExternalMapQueryContext<Integer, Integer, ?> nestedCxt;
                    // try lock different segment
                    while ((nestedCxt = map.queryContext(k)).segmentIndex() == keySegmentIndex) {
                        k++;
                    }
                    try (ExternalMapQueryContext<Integer, Integer, ?> nestedC = nestedCxt) {
                        nestedC.writeLock().lock();
                    } catch (IllegalStateException ex) {
                        // expected

                        // try lock the same segment
                        while ((nestedCxt = map.queryContext(k)).segmentIndex() !=
                                keySegmentIndex) {
                            k++;
                        }
                        try (ExternalMapQueryContext<Integer, Integer, ?> nestedC2 = nestedCxt) {
                            nestedC2.writeLock().lock();
                        } catch (IllegalStateException ex2) {
                            // expected
                            sawExpected[0] = true;
                            return;
                        }
                    }
                    throw new AssertionError("expected " + IllegalStateException.class);
                });
            } finally {
                queryCxt.close();
            }
        }
        assertTrue(sawExpected[0], "expected IllegalStateException for nested write locks");
    }

    @Test
    public void testNestedReadDifferentSegmentInIterationContextAllowed() {
        boolean[] visited = new boolean[1];
        try (ChronicleMap<Integer, Integer> map = ChronicleMapBuilder
                .of(Integer.class, Integer.class)
                .entries(100)
                .actualSegments(2)
                .create()) {
            map.put(42, 42);
            ExternalMapQueryContext<Integer, Integer, ?> queryCxt = map.queryContext(42);
            int keySegmentIndex = queryCxt.segmentIndex();
            try (MapSegmentContext<Integer, Integer, ?> cxt = map.segmentContext(keySegmentIndex)) {
                cxt.forEachSegmentEntry(e -> {
                    int k = 0;
                    ExternalMapQueryContext<Integer, Integer, ?> nestedCxt;
                    // try lock different segment
                    while ((nestedCxt = map.queryContext(k)).segmentIndex() == keySegmentIndex) {
                        k++;
                    }
                    try (ExternalMapQueryContext<Integer, Integer, ?> nestedC = nestedCxt) {
                        nestedC.readLock().lock();
                    }

                    // try lock the same segment
                    while ((nestedCxt = map.queryContext(k)).segmentIndex() !=
                            keySegmentIndex) {
                        k++;
                    }
                    try (ExternalMapQueryContext<Integer, Integer, ?> nestedC = nestedCxt) {
                        nestedC.readLock().lock();
                    }
                    visited[0] = true;
                });
            } finally {
                queryCxt.close();
            }
        }
        assertTrue(visited[0], "expected at least one entry visited");
    }

    @Test
    public void testNestedIterationInIterationContextForbidden() {
        assertThrows(IllegalStateException.class, () -> {
            try (ChronicleMap<Integer, Integer> map = ChronicleMapBuilder
                    .of(Integer.class, Integer.class)
                    .entries(100)
                    .create()) {
                map.put(42, 42);
                try (MapSegmentContext<Integer, Integer, ?> cxt = map.segmentContext(0)) {
                    cxt.forEachSegmentEntry(e -> map.segmentContext(1).forEachSegmentEntry(e2 -> {
                    }));
                }
            }
        });
    }
}
