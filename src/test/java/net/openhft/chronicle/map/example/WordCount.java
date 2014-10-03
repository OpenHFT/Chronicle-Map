package net.openhft.chronicle.map.example;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.lang.model.Byteable;
import net.openhft.lang.values.IntValue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

import static java.nio.charset.StandardCharsets.UTF_8;

public class WordCount {

    static String[] words;
    static int expectedSize;
    static {
        try {
            // english version of war and peace ->  ascii
            String fullText =
                    new String(Files.readAllBytes(Paths.get("war_and_peace.txt")), UTF_8);
            words = fullText.split("\\s+");
            expectedSize = (int) Arrays.stream(words).distinct().count();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static interface ByteableIntValue extends IntValue, Byteable {}

    public int chronicleMap() throws IOException {
        ChronicleMap<String, ByteableIntValue> map =
                ChronicleMapBuilder.of(String.class, ByteableIntValue.class)
                .entrySize(16)
                .actualSegments(1)
                .entries(expectedSize)
                .create();
        ByteableIntValue v = map.acquireUsing("a", null);
        for (String word : words) {
            map.acquireUsing(word, v).addValue(1);
        }
        return map.acquireUsing("a", v).getValue();
    }
}