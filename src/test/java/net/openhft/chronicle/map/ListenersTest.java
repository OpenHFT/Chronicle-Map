package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.Value;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ListenersTest {
    
    static class CountingEntryOperations<K, V> implements MapEntryOperations<K, V, Void> {
        AtomicInteger removeCount = new AtomicInteger();
        AtomicInteger insertCount = new AtomicInteger();
        AtomicInteger replaceValueCount = new AtomicInteger();
        @Override
        public Void remove(@NotNull MapEntry<K, V> entry) {
            removeCount.incrementAndGet();
            return MapEntryOperations.super.remove(entry);
        }
        @Override
        public Void replaceValue(@NotNull MapEntry<K, V> entry, Value<V, ?> newValue) {
            replaceValueCount.incrementAndGet();
            return MapEntryOperations.super.replaceValue(entry, newValue);
        }

        @Override
        public Void insert(@NotNull MapAbsentEntry<K, V> absentEntry, Value<V, ?> value) {
            insertCount.incrementAndGet();
            return MapEntryOperations.super.insert(absentEntry, value);
        }
    }
    
    @Test
    public void testRemove() {
        CountingEntryOperations<Integer, Integer> removeCounting =
                new CountingEntryOperations<>();
        ChronicleMap<Integer, Integer> map =
                ChronicleMapBuilder.of(Integer.class, Integer.class)
                .entries(100)
                .entryOperations(removeCounting)
                .create();
        
        map.put(1, 1);
        map.remove(1); // removeCount 1
        
        map.put(1, 1);
        assertFalse(map.remove(1, 2));
        map.remove(1, 1); // removeCount 2
        
        map.put(1, 1);
        map.merge(1, 1, (v1, v2) -> null); // removeCount 3
        
        map.put(1, 1);
        Iterator<Map.Entry<Integer, Integer>> it = map.entrySet().iterator();
        it.next();
        it.remove(); // removeCount 4

        assertEquals(4, removeCounting.removeCount.get());
    }

    @Test
    public void testPut() {
        CountingEntryOperations<Integer, Integer> putCounting =
                new CountingEntryOperations<>();
        ChronicleMap<Integer, Integer> map =
                ChronicleMapBuilder.of(Integer.class, Integer.class)
                        .entries(100)
                        .entryOperations(putCounting)
                        .create();

        map.put(1, 1); // insert 1

        map.put(1, 2); // replaceValue 1
        map.compute(1, (k, v) -> 2); // replaceValue 2
        
        map.entrySet().iterator().next().setValue(1); // replaceValue 3
        
        map.compute(2, (k, v) -> 1); // insert 2

        assertEquals(3, putCounting.replaceValueCount.get());
        assertEquals(2, putCounting.insertCount.get());
    }
}
