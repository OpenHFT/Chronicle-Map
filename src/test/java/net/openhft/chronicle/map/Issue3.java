package net.openhft.chronicle.map;

import net.openhft.chronicle.set.ChronicleSet;
import net.openhft.chronicle.set.ChronicleSetBuilder;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;
import java.util.Set;

public class Issue3 {

    @Test
    public void test() throws IOException {
        Set<Long> set = ChronicleSetBuilder.of(Long.class)
                .actualSegments(1)
                .actualEntriesPerSegment(1000)
                .create();

        Random r = new Random();
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 300; j++) {
                set.add(r.nextLong());
            }
            set.clear();
        }
    }
}
