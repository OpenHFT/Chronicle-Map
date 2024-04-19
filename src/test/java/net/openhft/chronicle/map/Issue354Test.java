package net.openhft.chronicle.map;

import net.openhft.chronicle.core.values.LongValue;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertNotNull;

public class Issue354Test {

    @Test
    public void reproduce() throws IOException {

        final File file = new File("issue354-map");
        file.delete();
        try {

            final ChronicleMapBuilder<LongValue, LongValue> builder = ChronicleMapBuilder.of(LongValue.class, LongValue.class)
                    .entries(5);

            try (ChronicleMap<LongValue, LongValue> map = builder.createPersistedTo(file)) {
                assertNotNull(map);
            }

        } finally {
            file.delete();
        }

    }

}
