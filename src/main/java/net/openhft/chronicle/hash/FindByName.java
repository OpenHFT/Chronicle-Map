package net.openhft.chronicle.hash;

import java.io.IOException;

/**
 * @author Rob Austin.
 */
public interface FindByName {

    /**
     * @param name the name of the map or set
     * @param <T>
     * @return a chronicle map or set
     * @throws IllegalArgumentException if a map with this name can not be found
     * @throws IOException              if it not possible to create the map or set
     */
    <T extends ChronicleHash> T create(String name) throws IllegalArgumentException,
            IOException;
}
