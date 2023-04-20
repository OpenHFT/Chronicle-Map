package net.openhft.chronicle.map;

import net.openhft.chronicle.bytes.CommonMarshallable;
import net.openhft.chronicle.core.ClassLocal;
import net.openhft.chronicle.core.util.ObjectUtils;

public class USDMUtil {
    public static final ClassLocal<Boolean> USDM = ClassLocal.withInitial(USDMUtil::usesSelfDescribingMessage);

    private static Boolean usesSelfDescribingMessage(Class<?> aClass) {
        Object value = ObjectUtils.newInstance(aClass);
        if (value instanceof CommonMarshallable)
            return ((CommonMarshallable) value).usesSelfDescribingMessage();
        throw new IllegalArgumentException();
    }
}
