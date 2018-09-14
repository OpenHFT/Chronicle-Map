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
