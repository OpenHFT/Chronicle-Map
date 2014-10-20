package net.openhft.chronicle.exceptions;

import java.io.IOException;

/**
 * @author Rob Austin.
 */
public class IORuntimeException extends RuntimeException{

    public IORuntimeException(IOException e) {
        super(e);
    }
}
