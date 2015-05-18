package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.Value;
import org.jetbrains.annotations.NotNull;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is designed to fire events when an entry in a <code>ChronicleMap</code></> is changed.
 * <code>ChronicleMapEventListener</code>'s can be added to listen for updates on all keys or for
 * updates on individual keys.
 */
public class ChronicleMapEventsProducer<K, V> implements MapEntryOperations<K, V> {
    private final Map<ChronicleMapEventListener, Set<K>> listeners = new ConcurrentHashMap<>();
    private final K ALL_EVENTS = (K)(new Object());

    /**
     * Add a listener which will be updated when any key on the map is updated
     * @param listener The listener which will be updated
     */
    public void addMapEventListener(ChronicleMapEventListener listener){
        addMapEventListener(listener, ALL_EVENTS);
    }

    /**
     * Add a listener which will be updated when a specific key is updated
     * @param listener The listener which will be updated
     * @param key The key on which the listener will be updated
     */
    public void addMapEventListener(ChronicleMapEventListener listener, K key){
        listeners.compute(listener, (k,v)-> {
            if(v==null)
                v = new HashSet<>();
            v.add(key);
            return v;
        });
    }

    /**
     * The listener will be removed for all the keys on which it has been
     * registered.
     * @param listener The listener which is to be removed
     */
    public void removeMapEventListener(ChronicleMapEventListener listener){
        listeners.remove(listener);
    }

    /**
     * The listener will no longer be updated for the specific key
     * @param listener The listener on which the key will not longer be updated
     * @param key The key that is to be removed from updates
     */
    public void removeMapEventListener(ChronicleMapEventListener listener, K key){
        listeners.compute(listener, (k,v)-> {
            if(v==null)
                return null;
            if(v.size()==0)
                return null;
            v.remove(key);
            return v;
        });
    }

    @Override
    public boolean replaceValue(@NotNull MapEntry<K, V> entry, Value<V, ?> newValue) {
        V oldEntry = entry.value().get();
        if (MapEntryOperations.super.replaceValue(entry, newValue)) {
            listeners.entrySet().stream()
                    .filter(e->e.getValue()==ALL_EVENTS || e.getValue().equals(entry.key().get()))
                    .forEach(e->{
                        if(oldEntry==null)
                            e.getKey().insert(entry.key().get(), newValue.get());
                        else
                            e.getKey().update(entry.key(),oldEntry, newValue.get());
                    });
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean remove(@NotNull MapEntry<K, V> entry){
        V oldEntry = entry.value().get();
        if(MapEntryOperations.super.remove(entry)){
            listeners.entrySet().stream()
                    .filter(e->e.getValue()==ALL_EVENTS || e.getValue().equals(entry.key().get()))
                    .forEach(e -> e.getKey().remove(entry.key(), oldEntry));
            return true;
        }else {
            return false;
        }
    }

    //todo move these into a test class.
    public static void main(String[] args) {
        ChronicleMapEventsProducer producer = new ChronicleMapEventsProducer();
        TestListener testListener = new TestListener();
        producer.addMapEventListener((key, oldValue, newValue) -> System.out.print(key));
        producer.addMapEventListener((key, oldValue, newValue) -> System.out.print(key), "test");
        producer.addMapEventListener(testListener);
        producer.addMapEventListener(testListener);
        producer.addMapEventListener(testListener, "test");
        producer.addMapEventListener(testListener, "test");

        producer.removeMapEventListener(testListener, "test");
        System.out.println("end");
    }

    private static class TestListener implements ChronicleMapEventListener{

        @Override
        public void update(Object key, Object oldValue, Object newValue) {
            System.out.print(key);
        }
    }
}
