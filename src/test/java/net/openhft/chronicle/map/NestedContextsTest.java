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

import com.google.common.collect.ImmutableSet;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NestedContextsTest {

    private static void verifyGraphConsistent(ChronicleMap<Integer, Set<Integer>> graph) {
        graph.forEach((node, neighbours) ->
                neighbours.forEach(neighbour -> assertTrue(graph.get(neighbour).contains(node))));
    }

    public static boolean addEdge(
            ChronicleMap<Integer, Set<Integer>> graph, int source, int target) {
        if (source == target)
            throw new IllegalArgumentException("loops are forbidden");
        ExternalMapQueryContext<Integer, Set<Integer>, ?> sourceC = graph.queryContext(source);
        ExternalMapQueryContext<Integer, Set<Integer>, ?> targetC = graph.queryContext(target);
        // order for consistent lock acquisition => avoid dead lock
        if (sourceC.segmentIndex() <= targetC.segmentIndex()) {
            return innerAddEdge(source, sourceC, target, targetC);
        } else {
            return innerAddEdge(target, targetC, source, sourceC);
        }
    }

    private static boolean innerAddEdge(
            int source, ExternalMapQueryContext<Integer, Set<Integer>, ?> sourceContext,
            int target, ExternalMapQueryContext<Integer, Set<Integer>, ?> targetContext) {
        try (ExternalMapQueryContext<Integer, Set<Integer>, ?> sc = sourceContext) {
            try (ExternalMapQueryContext<Integer, Set<Integer>, ?> tc = targetContext) {
                sc.updateLock().lock();
                tc.updateLock().lock();
                MapEntry<Integer, Set<Integer>> sEntry = sc.entry();
                if (sEntry != null) {
                    MapEntry<Integer, Set<Integer>> tEntry = tc.entry();
                    if (tEntry != null) {
                        return addEdgeBothPresent(sc, sEntry, source, tc, tEntry, target);
                    } else {
                        addEdgePresentAbsent(sc, sEntry, source, tc, target);
                        return true;
                    }
                } else {
                    MapEntry<Integer, Set<Integer>> tEntry = tc.entry();
                    if (tEntry != null) {
                        addEdgePresentAbsent(tc, tEntry, target, sc, source);
                    } else {
                        addEdgeBothAbsent(sc, source, tc, target);
                    }
                    return true;
                }
            }
        }
    }

    private static boolean addEdgeBothPresent(
            MapQueryContext<Integer, Set<Integer>, ?> sc,
            @NotNull MapEntry<Integer, Set<Integer>> sEntry, int source,
            MapQueryContext<Integer, Set<Integer>, ?> tc,
            @NotNull MapEntry<Integer, Set<Integer>> tEntry, int target) {
        Set<Integer> sNeighbours = sEntry.value().get();
        if (sNeighbours.add(target)) {
            Set<Integer> tNeighbours = tEntry.value().get();
            boolean added = tNeighbours.add(source);
            assert added;
            sEntry.doReplaceValue(sc.wrapValueAsData(sNeighbours));
            tEntry.doReplaceValue(tc.wrapValueAsData(tNeighbours));
            return true;
        } else {
            return false;
        }
    }

    private static void addEdgePresentAbsent(
            MapQueryContext<Integer, Set<Integer>, ?> sc,
            @NotNull MapEntry<Integer, Set<Integer>> sEntry, int source,
            MapQueryContext<Integer, Set<Integer>, ?> tc, int target) {
        Set<Integer> sNeighbours = sEntry.value().get();
        boolean added = sNeighbours.add(target);
        assert added;
        sEntry.doReplaceValue(sc.wrapValueAsData(sNeighbours));

        addEdgeOneSide(tc, source);
    }

    private static void addEdgeBothAbsent(MapQueryContext<Integer, Set<Integer>, ?> sc, int source,
                                          MapQueryContext<Integer, Set<Integer>, ?> tc, int target) {
        addEdgeOneSide(sc, target);
        addEdgeOneSide(tc, source);
    }

    private static void addEdgeOneSide(MapQueryContext<Integer, Set<Integer>, ?> tc, int source) {
        Set<Integer> tNeighbours = new HashSet<>();
        tNeighbours.add(source);
        MapAbsentEntry<Integer, Set<Integer>> tAbsentEntry = tc.absentEntry();
        assert tAbsentEntry != null;
        tAbsentEntry.doInsert(tc.wrapValueAsData(tNeighbours));
    }

    public static boolean removeEdge(
            ChronicleMap<Integer, Set<Integer>> graph, int source, int target) {
        ExternalMapQueryContext<Integer, Set<Integer>, ?> sourceC = graph.queryContext(source);
        ExternalMapQueryContext<Integer, Set<Integer>, ?> targetC = graph.queryContext(target);
        // order for consistent lock acquisition => avoid dead lock
        if (sourceC.segmentIndex() <= targetC.segmentIndex()) {
            return innerRemoveEdge(source, sourceC, target, targetC);
        } else {
            return innerRemoveEdge(target, targetC, source, sourceC);
        }
    }

    private static boolean innerRemoveEdge(
            int source, ExternalMapQueryContext<Integer, Set<Integer>, ?> sourceContext,
            int target, ExternalMapQueryContext<Integer, Set<Integer>, ?> targetContext) {
        try (ExternalMapQueryContext<Integer, Set<Integer>, ?> sc = sourceContext) {
            try (ExternalMapQueryContext<Integer, Set<Integer>, ?> tc = targetContext) {
                sc.updateLock().lock();
                MapEntry<Integer, Set<Integer>> sEntry = sc.entry();
                if (sEntry == null)
                    return false;
                Set<Integer> sNeighbours = sEntry.value().get();
                if (!sNeighbours.remove(target))
                    return false;

                tc.updateLock().lock();
                MapEntry<Integer, Set<Integer>> tEntry = tc.entry();
                if (tEntry == null)
                    throw new IllegalStateException("target node should be present in the graph");
                Set<Integer> tNeighbours = tEntry.value().get();
                if (!tNeighbours.remove(source))
                    throw new IllegalStateException("the target node have an edge to the source");
                sEntry.doReplaceValue(sc.wrapValueAsData(sNeighbours));
                tEntry.doReplaceValue(tc.wrapValueAsData(tNeighbours));
                return true;
            }
        }
    }

    @Test
    public void nestedContextsTest() throws ExecutionException, InterruptedException {
        HashSet<Integer> averageValue = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            averageValue.add(i);
        }
        ChronicleMap<Integer, Set<Integer>> graph = ChronicleMap
                .of(Integer.class, (Class<Set<Integer>>) (Class) Set.class)
                .entries(10)
                .averageValue(averageValue)
                .actualSegments(2)
                .create();

        addEdge(graph, 1, 2);
        addEdge(graph, 2, 3);
        addEdge(graph, 1, 3);

        assertEquals(ImmutableSet.of(2, 3), graph.get(1));
        assertEquals(ImmutableSet.of(1, 3), graph.get(2));
        assertEquals(ImmutableSet.of(1, 2), graph.get(3));

        verifyGraphConsistent(graph);

        ForkJoinPool pool = new ForkJoinPool(8);
        try {
            pool.submit(() -> {
                ThreadLocalRandom.current().ints().limit(10_000).parallel().forEach(i -> {
                    int sourceNode = Math.abs(i % 10);
                    int targetNode;
                    do {
                        targetNode = ThreadLocalRandom.current().nextInt(10);
                    } while (targetNode == sourceNode);
                    if (i % 2 == 0) {
                        addEdge(graph, sourceNode, targetNode);
                    } else {
                        removeEdge(graph, sourceNode, targetNode);
                    }
                });
            }).get();
            verifyGraphConsistent(graph);
        } finally {
            pool.shutdownNow();
        }
    }
}
