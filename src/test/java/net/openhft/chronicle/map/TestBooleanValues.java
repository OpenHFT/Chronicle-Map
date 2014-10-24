/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import java.io.IOException;


/**
 * @author Rob Austin.
 */
public class TestBooleanValues {

    /**
     * see issue http://stackoverflow.com/questions/26219313/strange-npe-from-chronicle-map-toy-code
     */
    @Test
    public void testTestBooleanValues() throws IOException, InterruptedException {
        ChronicleMap<Integer, Boolean> map =
                ChronicleMapBuilder.of(Integer.class, Boolean.class).create();
        map.put(7, true);
        Assert.assertEquals(true, map.get(7));
    }
}

