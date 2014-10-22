package net.openhft.chronicle.common.exceptions;

/**
 * @author Rob Austin.
 */
public class TimeoutRuntimeException extends RuntimeException {


    public TimeoutRuntimeException(String s) {
        super(s);
    }

    public TimeoutRuntimeException() {
        super();
    }
}
