package net.openhft.chronicle.map;

/**
 * @author Rob Austin.
 */
public interface SegmentLock extends AutoCloseable {

    /**
     * call this to unlock the segment
     */
    void close();

}
