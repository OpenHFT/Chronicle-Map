package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.Value;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BiMapTest {
    
    static class BiMapEntryOperations<K, V> implements MapEntryOperations<K, V, Void> {
        ChronicleMap<V, K> reverse;

        public void setReverse(ChronicleMap<V, K> reverse) {
            this.reverse = reverse;
        }

        @Override
        public Void remove(@NotNull MapEntry<K, V> entry) {
            try (ExternalMapQueryContext<V, K, ?> rq = reverse.queryContext(entry.value().get())) {
                rq.updateLock().lock();
                MapEntry<V, K> reverseEntry = rq.entry();
                if (reverseEntry != null) {
                    entry.doRemove();
                    reverseEntry.doRemove();
                    return null;
                } else {
                    throw new IllegalStateException(entry.key() + " maps to " + entry.value() +
                            ", but in the reverse map this value is absent");
                }
            }
        }

        @Override
        public Void replaceValue(@NotNull MapEntry<K, V> entry, Value<V, ?> newValue) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Void insert(@NotNull MapAbsentEntry<K, V> absentEntry, Value<V, ?> value) {
            try (ExternalMapQueryContext<V, K, ?> rq = reverse.queryContext(value.get())) {
                rq.updateLock().lock();
                MapAbsentEntry<V, K> reverseAbsentEntry = rq.absentEntry();
                if (reverseAbsentEntry != null) {
                    absentEntry.doInsert(value);
                    reverseAbsentEntry.doInsert(absentEntry.absentKey());
                    return null;
                } else {
                    Value<K, ?> reverseKey = rq.entry().value();
                    if (reverseKey.equals(absentEntry.absentKey())) {
                        // recover
                        return null;
                    }
                    throw new IllegalArgumentException("Try to associate " +
                            absentEntry.absentKey() + " with " + value + ", but in the reverse " +
                            "map this value already maps to " + reverseKey);
                }
            }
        }
    }
    
    @Test
    public void biMapTest() {
        BiMapEntryOperations<Integer, CharSequence> biMapOps1 = new BiMapEntryOperations<>();
        ChronicleMap<Integer, CharSequence> map1 = ChronicleMapBuilder
                .of(Integer.class, CharSequence.class)
                .entries(100)
                .averageValueSize(10)
                .entryOperations(biMapOps1)
                .create();

        BiMapEntryOperations<CharSequence, Integer> biMapOps2 = new BiMapEntryOperations<>();
        ChronicleMap<CharSequence, Integer> map2 = ChronicleMapBuilder
                .of(CharSequence.class, Integer.class)
                .entries(100)
                .averageKeySize(10)
                .entryOperations(biMapOps2)
                .create();

        biMapOps1.setReverse(map2);
        biMapOps2.setReverse(map1);

        map1.put(1, "1");
        assertEquals(1, map2.get("1").intValue());
        
        map2.remove("1");
        assertTrue(map2.isEmpty());
        assertTrue(map1.isEmpty());
        
        map1.put(3, "4");
        map2.put("5", 6);
        assertEquals(2, map1.size());

        try (ExternalMapQueryContext<CharSequence, Integer, ?> q = map2.queryContext("4")) {
            q.updateLock().lock();
            q.entry().doRemove();
        }
        
        try {
            map1.remove(3);
            throw new AssertionError("expected IllegalStateException");
        } catch (IllegalStateException e) {
            // expected
        }
        
        try {
            map2.put("4", 6);
            throw new AssertionError("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        }
        
        map2.put("4", 3); // recover
    }
}
