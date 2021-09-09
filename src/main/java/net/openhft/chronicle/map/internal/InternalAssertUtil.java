package net.openhft.chronicle.map.internal;

import net.openhft.chronicle.core.Jvm;

public final class InternalAssertUtil {

    private static final boolean IS_64_BIT = Jvm.is64bit();

    // Suppresses default constructor, ensuring non-instantiability.
    private InternalAssertUtil() {
    }

    public static boolean assertAddress(final long address) {
        if (Jvm.is64bit()) {
            // It is highly unlikely that we would ever address farther than 2^63
            assert address > 0 : "address is negative: " + address;
        } else {
            // These memory addresses are illegal on a 32-bit machine
            assert address != 0 && address != -1 : "address is illegal: " + address;
        }
        return true;
    }

    public static boolean assertPosition(final long position) {
        assert position > 0 : "position is negative: " + position;
        return true;
    }

}
