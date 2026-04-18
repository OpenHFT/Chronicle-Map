/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.util.Time;
import org.junit.Test;

import java.io.*;
import java.util.Map;

import static org.junit.Assert.assertSame;

@SuppressWarnings({"rawtypes", "unchecked"})
public class RecursiveRefereneChMapTest {
    public static final String TMP = OS.getTarget();

    @Test
    public void testRecursive() throws IOException {
        File file = new File(TMP + "/test." + Time.uniqueId() + ".tmp");
        file.deleteOnExit();
        Map<String, StupidCycle> map = ChronicleMapBuilder.of(String.class, StupidCycle.class)
                .averageKey("Test").averageValue(new StupidCycle())
                .entries(64)
                .create();
        map.put("Test", new StupidCycle());
        map.put("Test2", new StupidCycle2());
        StupidCycle cycle = map.get("Test");
        assertSame(cycle, cycle.cycle[0]);
        StupidCycle cycle2 = map.get("Test2");
        assertSame(cycle2, cycle2.cycle[0]);
    }

    @SuppressWarnings("serial")
    public static class StupidCycle implements Serializable {
        int dummy;
        Object[] cycle = {this};
    }

    @SuppressWarnings("serial")
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
