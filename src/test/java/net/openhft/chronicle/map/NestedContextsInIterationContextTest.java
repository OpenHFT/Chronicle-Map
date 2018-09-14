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

import org.junit.Test;

import static org.junit.Assert.assertNull;

public class NestedContextsInIterationContextTest {

    @Test
    public void testNestedGetsInIterationContextAllowed() {
        try (ChronicleMap<Integer, Integer> map = ChronicleMapBuilder
                .of(Integer.class, Integer.class)
                .entries(100)
                .create()) {
            map.put(42, 42);
            map.forEachEntry(e -> assertNull(map.get(0)));
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testNestedPutsInIterationContextForbidden() {
        try (ChronicleMap<Integer, Integer> map = ChronicleMapBuilder
                .of(Integer.class, Integer.class)
                .entries(100)
                .create()) {
            map.put(42, 42);
            map.forEachEntry(e -> map.put(0, 0));
        }
    }

    @Test
    public void testNestedUpdatesDifferentSegmentInIterationContextForbidden() {
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
                            return;
                        }
                    }
                    throw new AssertionError("expected " + IllegalStateException.class);
                });
            } finally {
                queryCxt.close();
            }
        }
    }

    @Test
    public void testNestedWritesDifferentSegmentInIterationContextForbidden() {
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
                            return;
                        }
                    }
                    throw new AssertionError("expected " + IllegalStateException.class);
                });
            } finally {
                queryCxt.close();
            }
        }
    }

    @Test
    public void testNestedReadDifferentSegmentInIterationContextAllowed() {
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
                });
            } finally {
                queryCxt.close();
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testNestedIterationInIterationContextForbidden() {
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
    }
}
