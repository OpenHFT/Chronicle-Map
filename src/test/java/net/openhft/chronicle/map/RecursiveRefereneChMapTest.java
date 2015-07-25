/*
 *      Copyright (C) 2015  higherfrequencytrading.com
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

import net.openhft.lang.io.serialization.JDKObjectSerializer;
import org.junit.Test;

import java.io.*;
import java.util.Map;

import static org.junit.Assert.assertSame;

public class RecursiveRefereneChMapTest {
    public static final String TMP = System.getProperty("java.io.tmpdir");

    @Test
    public void testRecursive() throws IOException {
        File file = new File(TMP + "/test." + System.nanoTime() + ".tmp");
        file.deleteOnExit();
        Map<String, StupidCycle> map = ChronicleMapBuilder.of(String.class, StupidCycle.class)
                .entries(64)
                .objectSerializer(JDKObjectSerializer.INSTANCE).create();
        map.put("Test", new StupidCycle());
        map.put("Test2", new StupidCycle2());
        StupidCycle cycle = (StupidCycle) map.get("Test");
        assertSame(cycle, cycle.cycle[0]);
        StupidCycle cycle2 = (StupidCycle) map.get("Test2");
        assertSame(cycle2, cycle2.cycle[0]);
    }

    public static class StupidCycle implements Serializable {
        int dummy;
        Object cycle[] = {this};
    }

    public static class StupidCycle2 extends StupidCycle implements Externalizable {
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(dummy);
            out.writeObject(cycle);
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            dummy = in.readInt();
            cycle = (Object[]) in.readObject();
        }
    }
}
