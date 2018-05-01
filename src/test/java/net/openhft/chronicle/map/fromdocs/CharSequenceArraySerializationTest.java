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

package net.openhft.chronicle.map.fromdocs;

import net.openhft.chronicle.map.ChronicleMap;
import org.junit.Assert;
import org.junit.Test;

public class CharSequenceArraySerializationTest {

    @Test
    public void charSequenceArraySerializationTest() {
        try (ChronicleMap<String, CharSequence[]> map = ChronicleMap
                .of(String.class, CharSequence[].class)
                .averageKey("fruits")
                .valueMarshaller(CharSequenceArrayBytesMarshaller.INSTANCE)
                .averageValue(new CharSequence[]{"banana", "pineapple"})
                .entries(2)
                .create()) {
            map.put("fruits", new CharSequence[]{"banana", "pineapple"});
            map.put("vegetables", new CharSequence[]{"carrot", "potato"});
            Assert.assertEquals(2, map.get("fruits").length);
            Assert.assertEquals(2, map.get("vegetables").length);
        }
    }
}
