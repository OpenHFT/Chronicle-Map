//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

package net.openhft.chronicle.map;

import org.junit.Assert;
import org.junit.Test;

public class DeletedSearchStateTest {

    @Test
    public void deletedSearchStateTest() {
        ChronicleMap<Integer, Integer> map =
                ChronicleMapBuilder.of(Integer.class, Integer.class).entries(100).create();

        try (ExternalMapQueryContext<Integer, Integer, ?> q = map.queryContext(42)) {
            q.updateLock().lock();
            q.insert(q.absentEntry(), q.wrapValueAsData(1));
            q.remove(q.entry());
            q.insert(q.absentEntry(), q.wrapValueAsData(2));
        }

        Assert.assertEquals((Integer) 2, map.get(42));
    }
}
