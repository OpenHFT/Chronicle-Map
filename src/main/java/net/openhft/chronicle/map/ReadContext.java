package net.openhft.chronicle.map;

/**
 * @author Rob Austin.
 */
public interface ReadContext<K,V> extends Context<K,V> {

    boolean present();

}
