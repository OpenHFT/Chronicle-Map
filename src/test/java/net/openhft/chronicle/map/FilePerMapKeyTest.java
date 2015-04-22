package net.openhft.chronicle.map;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * JUnit test class to support {@link net.openhft.chronicle.map.FilePerMapKeyTest}
 */
public class FilePerMapKeyTest {
    /**
     * FilePerMapKey only works with Strings
     */
    @Test(expected = AssertionError.class)
    public void testWrongType(){
        FilePerKeyMap<Integer, String> map = new FilePerKeyMap<>("/tmp/filepermaptests");
        map.put(1, "test");
    }

    /**
     * Testing the put
     */
    @Test
    public void testPut(){
        //There are no entries in the map so null should be returned
        FilePerKeyMap<String, String> map = new FilePerKeyMap<>("/tmp/filepermaptests");
        //just in case it hasn't been cleared up last time
        map.clear();

        String val = map.put("one", "test1");
        assertEquals(val, null);

        //After the entry has been written the value returned should be
        //the previous value
        val = map.put("one", "test2");
        assertEquals(val, "test1");

        //Check that get returns the latest value
        val = map.get("one");
        assertEquals("test2", val);

        assertTrue(map.containsKey("one"));
        assertFalse(map.containsKey("two"));

        //Check the size is one
        int size = map.size();
        assertEquals(1,size);
        assertFalse(map.isEmpty());

        map.clear();
        //Check the size is now 0
        size = map.size();
        assertEquals(0, size);

        assertTrue(map.isEmpty());
    }
}
