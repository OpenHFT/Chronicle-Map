/*
 * Copyright 2014 Higher Frequency Trading http://www.higherfrequencytrading.com
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

import net.openhft.chronicle.map.fromdocs.BondVOInterface;
import net.openhft.lang.model.DataValueGenerator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.hasItem;

public class FirstPrimitiveFieldTest {
    @Test
    public void firstPrimitiveFieldTest() {
        Assert.assertThat(Arrays.<Class>asList(long.class, double.class),
                hasItem(DataValueGenerator.firstPrimitiveFieldType(BondVOInterface.class)));
    }
}
