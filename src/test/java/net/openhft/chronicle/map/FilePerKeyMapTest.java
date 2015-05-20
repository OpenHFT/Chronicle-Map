package net.openhft.chronicle.map;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;

import static org.junit.Assert.*;

/**
 * JUnit test class to support {@link FilePerKeyMapTest}
 */
public class FilePerKeyMapTest {
    private static FilePerKeyMap map;

    @BeforeClass
    public static void createMap() throws IOException {
        map = new FilePerKeyMap("/tmp/filepermaptests");
        //just in case it hasn't been cleared up last time
        map.clear();
    }

    /**
     * Testing all the methods of the map with simple tests.
     */
    @Test
    public void testMapMethods() throws InterruptedException {
        String val = map.put("one", "test1");
        assertEquals(val, null);
        Thread.sleep(5);

        //After the entry has been written the value returned should be
        //the previous value
        val = map.put("one", "test2");
        assertEquals(val, "test1");

        //Check that get returns the latest value
        val = map.get("one");
        assertEquals("test2", val);

        assertTrue(map.containsKey("one"));
        assertFalse(map.containsKey("two"));
        assertTrue(map.containsValue("test2"));
        assertFalse(map.containsValue("test5"));

        //Check the size is one
        int size = map.size();
        assertEquals(1, size);
        assertFalse(map.isEmpty());

        map.clear();
        Thread.sleep(5);

        //Check the size is now 0
        size = map.size();
        assertEquals(0, size);

        assertTrue(map.isEmpty());

        map.put("one", "test1");
        map.put("two", "test2");
        map.put("three", "test3");
        assertEquals(3, map.size());

        val = map.remove("two");
        assertEquals("test2", val);

        assertEquals(2, map.size());

        val = map.remove("four");//doesn't exist
        assertEquals(null, val);

        map.clear();

        Map<String, String> copyFrom = new HashMap<>();
        copyFrom.put("five", "test5");
        copyFrom.put("six", "test6");
        copyFrom.put("seven", "test7");
        map.putAll(copyFrom);
        assertEquals(3, map.size());

        Set<String> set = map.keySet();
        assertEquals(3, set.size());
        assertTrue(set.contains("five"));
        assertTrue(set.contains("six"));
        assertTrue(set.contains("seven"));

        set = (Set) map.values();
        assertEquals(3, set.size());
        assertTrue(set.contains("test5"));
        assertTrue(set.contains("test6"));
        assertTrue(set.contains("test7"));

        Set<Map.Entry> entryset = (Set) map.entrySet();
        assertEquals(3, entryset.size());
        for (Iterator<Map.Entry> it = entryset.iterator(); it.hasNext(); ) {
            Map.Entry entry = it.next();
            if (entry.getKey().equals("five")) {
                assertEquals(entry.getValue(), "test5");
            } else if (entry.getKey().equals("six")) {
                assertEquals(entry.getValue(), "test6");
            } else if (entry.getKey().equals("seven")) {
                assertEquals(entry.getValue(), "test7");
            } else {
                //should never get here!!
                assertTrue(false);
            }
        }

        map.clear();
    }

    @Test
    public void perfTest() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 2_000_000; i++) {
            sb.append('x');
        }
        String value = sb.toString();

        //small warm-up
        for (int i = 0; i < 10; i++) {
            map.put("big file", value);
        }


        long time = System.currentTimeMillis();
        int iterations = 50;
        for (int i = 0; i < iterations; i++) {
            map.put("big file", value);
        }
        System.out.println("Time to update " + iterations + " iterations " + (System.currentTimeMillis() - time));

        map.clear();
    }

    @Ignore //This doesn't work on all OS
    @Test
    public void eventTest() throws InterruptedException {
        final List<FPMEvent> events = new ArrayList<>();

        Consumer<FPMEvent> fpmEventConsumer = (FPMEvent e) -> {
            System.out.println(e);
            events.add(e);
        };
        map.registerForEvents(fpmEventConsumer);

        map.put("one", "one");
        map.put("one", "two");
        map.remove("one");
        map.put("one", "one");

        //Allow all events to be played through
        Thread.sleep(3000);
        //Check the events
        assertEquals(4, events.size());

        assertEquals(FPMEvent.EventType.NEW, events.get(0).getEventType());
        assertEquals(FPMEvent.EventType.UPDATE, events.get(1).getEventType());
        assertEquals(FPMEvent.EventType.DELETE, events.get(2).getEventType());
        assertEquals(FPMEvent.EventType.NEW, events.get(3).getEventType());


        map.unregisterForEvents(fpmEventConsumer);
        map.clear();
    }
}
