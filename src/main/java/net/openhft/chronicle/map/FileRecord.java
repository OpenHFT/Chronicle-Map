package net.openhft.chronicle.map;

/**
 * Created by daniel on 21/05/15.
 */
class FileRecord<T> {
    final long timestamp;
    boolean valid = true;
    final T contents;

    FileRecord(long timestamp, T contents) {
        this.timestamp = timestamp;
        this.contents = contents;
    }
}
