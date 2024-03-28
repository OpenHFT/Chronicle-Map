package net.openhft.chronicle.map;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.wire.BytesInBinaryMarshallable;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class ForEachSegmentTest {
    static {
        ClassAliasPool.CLASS_ALIASES.addAlias(MyDto.class);
    }

    @Test
    public void forEachSegmentEntryWhileReleasesLock() throws IOException {
        ChronicleMapBuilder<Integer, MyDto> builder = ChronicleMapBuilder.simpleMapOf(Integer.class, MyDto.class)
                .entries(256)
                .actualSegments(1);
        File tmp = new File(OS.TMP, "stressTest-" + Time.uniqueId());
        tmp.deleteOnExit();
        try (ChronicleMap<Integer, MyDto> map = builder.createPersistedTo(tmp)) {
            map.put(1, new MyDto());
            try (MapSegmentContext<Integer, MyDto, ?> context = map.segmentContext(0)) {
                context.forEachSegmentEntryWhile(e -> {
                    System.out.println(e.key().get() + " = " + e.value().get());
                    return true;
                });
            }
            System.out.println("Done");
        }
    }

    @Test
    public void stressTest() throws IOException, InterruptedException {
        ChronicleMapBuilder<Integer, MyDto> builder = ChronicleMapBuilder.simpleMapOf(Integer.class, MyDto.class)
                .entries(256)
                .actualSegments(1);
        File tmp = new File(OS.TMP, "stressTest-" + Time.uniqueId());
        Thread t = null;
        try (ChronicleMap<Integer, MyDto> map = builder.createPersistedTo(tmp)) {
            try {
                t = new Thread(() -> {
                    try {
                        for (int i = 0; i < 100; i++) {
                            System.out.println("put " + i);
                            map.put(i, new MyDto());
                            Thread.sleep(10);
                        }
                    } catch (InterruptedException e) {
                        System.out.println("Interrupted");
                    }
                });
                t.setDaemon(true);
                t.start();
                Jvm.pause(100);
                try (MapSegmentContext<Integer, MyDto, ?> context = map.segmentContext(0)) {
                    context.forEachSegmentEntryWhile(e -> {
                        System.out.println(e.key().get() + " = " + e.value().get());
                        Jvm.pause(10);
                        return true;
                    });
                }
                System.out.println("Done");
                Jvm.pause(100);
            } catch (Throwable th) {
                th.printStackTrace();
            } finally {
                if (t != null) {
                    t.interrupt();
                    t.join();
                }
            }
        }
    }

    static final class MyDto extends BytesInBinaryMarshallable {
        String s1;
        String s2;
    }
}
