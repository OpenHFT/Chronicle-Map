/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
