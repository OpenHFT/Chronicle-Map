package net.openhft.chronicle.map;

/**
 * Created by daniel on 23/04/15.
 */
public class FPMEvent {
    private EventType eventType;
    private String key;
    private String value;
    private String lastValue;

    public FPMEvent(EventType eventType, String key, String lastValue, String value) {
        this.eventType = eventType;
        this.key = key;
        this.value = value;
        this.lastValue = lastValue;
    }

    public String getLastValue() {
        return lastValue;
    }

    public void setLastValue(String lastValue) {
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

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
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
