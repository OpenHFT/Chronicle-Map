package net.openhft.chronicle.map;

/**
 * Created by daniel on 23/04/15.
 */
public class FPMEvent<V> {
    private EventType eventType;
    private String key;
    private V value;
    private V lastValue;

    public FPMEvent(EventType eventType, String key, V lastValue, V value) {
        this.eventType = eventType;
        this.key = key;
        this.value = value;
        this.lastValue = lastValue;
    }

    public V getLastValue() {
        return lastValue;
    }

    public void setLastValue(V lastValue) {
        this.lastValue = lastValue;
    }

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public V getValue() {
        return value;
    }

    public void setValue(V value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "FPMEvent{" +
                "eventType=" + eventType +
                ", key='" + key + '\'' +
                ", value='" + value + '\'' +
                ", lastValue='" + lastValue + '\'' +
                '}';
    }

    public enum EventType {NEW, UPDATE, DELETE}
}
