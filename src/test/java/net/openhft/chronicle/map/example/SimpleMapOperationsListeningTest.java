package net.openhft.chronicle.map.example;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.map.*;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.values.IntValue;
import org.jetbrains.annotations.NotNull;
import org.junit.Ignore;
import org.junit.Test;

import static net.openhft.chronicle.map.example.SimpleMapOperationsListeningTest.SimpleLoggingMapEntryOperations.simpleLoggingMapEntryOperations;

public class SimpleMapOperationsListeningTest {

    static class SimpleLoggingMapEntryOperations<K, V> implements MapEntryOperations<K, V, Void> {

        private static final SimpleLoggingMapEntryOperations INSTANCE =
                new SimpleLoggingMapEntryOperations();

        public static <K, V> MapEntryOperations<K, V, Void> simpleLoggingMapEntryOperations() {
            return SimpleLoggingMapEntryOperations.INSTANCE;
        }

        private SimpleLoggingMapEntryOperations() {}

        @Override
        public Void remove(@NotNull MapEntry<K, V> entry) {
            System.out.println("remove " + entry.key() + ": " + entry.value());
            entry.doRemove();
            return null;
        }

        @Override
        public Void replaceValue(@NotNull MapEntry<K, V> entry, Data<V> newValue) {
            System.out.println("replace " + entry.key() + ": " + entry.value() + " -> " + newValue);
            entry.doReplaceValue(newValue);
            return null;
        }

        @Override
        public Void insert(@NotNull MapAbsentEntry<K, V> absentEntry, Data<V> value) {
            System.out.println("insert " + absentEntry.absentKey() + " -> " + value);
            absentEntry.doInsert(value);
            return null;
        }

        @Override
        public Data<V> defaultValue(@NotNull MapAbsentEntry<K, V> absentEntry) {
            Data<V> defaultValue = absentEntry.defaultValue();
            System.out.println("default " + absentEntry.absentKey() + " -> " + defaultValue);
            return defaultValue;
        }
    }

    @Test
    @Ignore("Example test, run manually to see the output")
    public void simpleLoggingTest() {
        ChronicleMap<Integer, IntValue> map = ChronicleMapBuilder
                .of(Integer.class, IntValue.class)
                .entries(100)
                .entryOperations(simpleLoggingMapEntryOperations())
                .create();

        IntValue value = DataValueClasses.newDirectInstance(IntValue.class);
        value.setValue(2);
        map.put(1, value);
        map.remove(1);
        map.acquireUsing(3, value).addAtomicValue(1);
        IntValue value2 = DataValueClasses.newDirectInstance(IntValue.class);
        value2.setValue(5);
        map.forEachEntry(c -> c.put(value2));
        map.forEachEntry(c -> c.remove());

    }
}
