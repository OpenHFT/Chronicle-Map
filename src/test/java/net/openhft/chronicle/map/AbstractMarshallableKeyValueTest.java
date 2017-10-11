package net.openhft.chronicle.map;

import net.openhft.chronicle.wire.AbstractMarshallable;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public final class AbstractMarshallableKeyValueTest {

    @Test
    public void shouldAcceptAbstractMarshallableComponents() throws Exception {
        final ChronicleMap<Key, Value> map = ChronicleMapBuilder.of(Key.class, Value.class).entries(10).
                averageKey(new Key()).averageValue(new Value()).create();

        map.put(new Key(), new Value());

        assertThat(map.get(new Key()).number, is(new Value().number));
    }

    private static final class Key extends AbstractMarshallable {
        private String k = "key";
    }

    private static final class Value extends AbstractMarshallable {
        private Integer number = Integer.valueOf(17);
    }
}
