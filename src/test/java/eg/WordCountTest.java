/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package eg;

import com.google.common.io.ByteStreams;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.values.IntValue;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.values.Values;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.reducing;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class WordCountTest {

    static final String[] words;
    static final Map<CharSequence, Integer> expectedMap;

    static {
        // english version of war and peace ->  ascii
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try (InputStream zippedIS = Objects.requireNonNull(cl.getResourceAsStream("war_and_peace.txt.gz"));
             GZIPInputStream binaryIS = new GZIPInputStream(zippedIS)) {
            String fullText =
                    new String(ByteStreams.toByteArray(binaryIS), UTF_8);
            words = fullText.split("\\s+");
            expectedMap = Arrays.stream(words)
                    .map(CharSequence.class::cast)
                    .collect(groupingBy(
                            Function.identity(),
                            reducing(0, e -> 1, Integer::sum))
                    );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    ///@Ignore("https://github.com/OpenHFT/Chronicle-Map/issues/376")
    @Test
    public void wordCountTest() {
        try (ChronicleMap<CharSequence, IntValue> map = ChronicleMap
                .of(CharSequence.class, IntValue.class)
                .averageKeySize(7) // average word is 7 ascii bytes long (text in english)
                .entries(expectedMap.size())
                .create()) {
            IntValue v = Values.newNativeReference(IntValue.class);

            for (String word : words) {
                try (Closeable ignored = map.acquireContext(word, v)) {
                    assertNotNull(ignored);
                    v.addValue(1);
                }
            }

            assertEquals(expectedMap.size(), map.size());
            expectedMap.forEach((key, value) ->
                    assertEquals((int) value, map.get(key).getValue())
            );
        }
    }
}
