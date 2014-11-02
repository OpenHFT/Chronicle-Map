package net.openhft.chronicle.map;

/**
 * @author Rob Austin.
 */
public interface WriteContext<K, V> extends Context<K, V> {
    void dontPutOnClose();

    void removeEntry();

    // todo I have no idea why we have this, as this will aways be done on when calling close()
    // void update();
}
