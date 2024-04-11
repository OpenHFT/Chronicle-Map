package net.openhft.chronicle.map.channel;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.hash.serialization.impl.BytesSizedMarshaller;
import net.openhft.chronicle.hash.serialization.impl.MarshallableReaderWriter;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.Wires;
import net.openhft.chronicle.wire.channel.ChronicleChannel;
import net.openhft.chronicle.wire.channel.ChronicleContext;
import net.openhft.chronicle.wire.utils.YamlTester;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class MapHandlerTest {
    @Test
    public void passThroughMapFF() throws IOException {
        doPassThroughMap(false, false);
    }

    @Test
    public void passThroughMapFT() throws IOException {
        doPassThroughMap(false, true);
    }

    @Test
    @Ignore(/*TODO FIX*/)
    public void passThroughMapTF() throws IOException {
        doPassThroughMap(true, false);
    }

    @Test
    @Ignore(/*TODO FIX*/)
    public void passThroughMapTT() throws IOException {
        doPassThroughMap(true, true);
    }

    public void doPassThroughMap(boolean serverBuffered, boolean clientBuffered) throws IOException {
        String ns = "passThroughMap";

        PassMapService mapService = new PassMapService();
        MapHandler<DTO, Reply> mapHandler = MapHandler.createMapHandler(ns, mapService);

        try (ChronicleContext cc = ChronicleContext.newContext("tcp://:65301").buffered(serverBuffered);
             ChronicleMap<Bytes<?>, DTO> map = createMap(cc, ns);
             ChronicleChannel channel = cc.newChannelSupplier(mapHandler).buffered(clientBuffered).get()) {
            cc.toFile(ns + ".cm3").deleteOnExit();
            final PassMapServiceIn serviceIn = channel.methodWriter(PassMapServiceIn.class);

            final Bytes<byte[]> one = Bytes.from("one");
            final Bytes<byte[]> two = Bytes.from("two");
            final Bytes<byte[]> three = Bytes.from("three");
            final Bytes<byte[]> four = Bytes.from("four");
            serviceIn.put(one, new DTO("1"));
            serviceIn.put(two, new DTO("22"));
            serviceIn.put(three, new DTO("333"));
            serviceIn.get(one);
            serviceIn.get(two);
            serviceIn.get(three);
            serviceIn.get(four);
            serviceIn.remove(three);
            serviceIn.remove(four);
            serviceIn.get(two);
            serviceIn.get(three);
            serviceIn.goodbye();

            Wire wire = Wire.newYamlWireOnHeap();
            Reply reply2 = wire.methodWriter(Reply.class);
            MethodReader reader = channel.methodReader(reply2);
            for (int i = 0; i < 12; i++) {
                if (!reader.readOne())
                    i--;
            }
            try {
                try (DocumentContext dc = channel.readingDocument()) {
                    if (dc.isPresent()) {
                        fail(Wires.fromSizePrefixedBlobs(dc));
                    }
                }
            } catch (IORuntimeException expected) {
            }
            //noinspection YAMLDuplicatedKeys
            assertEquals("" +
                            "status: true\n" +
                            "...\n" +
                            "status: true\n" +
                            "...\n" +
                            "status: true\n" +
                            "...\n" +
                            "reply: {\n" +
                            "  text: \"1\"\n" +
                            "}\n" +
                            "...\n" +
                            "reply: {\n" +
                            "  text: \"22\"\n" +
                            "}\n" +
                            "...\n" +
                            "reply: {\n" +
                            "  text: \"333\"\n" +
                            "}\n" +
                            "...\n" +
                            "reply: !!null \"\"\n" +
                            "...\n" +
                            "status: true\n" +
                            "...\n" +
                            "status: false\n" +
                            "...\n" +
                            "reply: {\n" +
                            "  text: \"22\"\n" +
                            "}\n" +
                            "...\n" +
                            "reply: !!null \"\"\n" +
                            "...\n" +
                            "goodbye: \"\"\n" +
                            "...\n",
                    wire.toString());
        }
    }

    @Test
    public void passThroughServiceYaml() throws IOException {
        String ns = "passThroughServiceYaml";
        new File(ns + ".cm3").deleteOnExit();
        try (PassMapService mapService = new PassMapService();
             ChronicleMap<Bytes<?>, DTO> map = createMap(null, ns)) {
            mapService.map(map);
            final YamlTester yamlTester = YamlTester.runTest(out -> {
                mapService.reply(out);
                return mapService;
            }, Reply.class, "pass-through");
            assertEquals(yamlTester.expected(), yamlTester.actual());
        }
    }

    private ChronicleMap<Bytes<?>, DTO> createMap(ChronicleContext context, String namespace) throws IOException {
        final MarshallableReaderWriter<DTO> valueMarshaller = new MarshallableReaderWriter<>(DTO.class);
        final File file = context == null ? new File(namespace + ".cm3") : context.toFile(namespace + ".cm3");
        final Class<Bytes<?>> keyClass = (Class) Bytes.class;
        return ChronicleMap.of(keyClass, DTO.class)
                .averageKeySize(128)
                .averageValueSize(1 << 10)
                .entries(128 << 10)
                .sparseFile(true)
                .keyMarshaller(new BytesSizedMarshaller())
                .valueMarshaller(valueMarshaller)
                .createPersistedTo(file);
    }

    interface PassMapServiceIn extends Closeable {
        void put(Bytes<?> key, DTO value);

        void get(Bytes<?> key);

        void remove(Bytes<?> key);

        void goodbye();
    }

    interface Reply {
        void status(boolean ok);

        void reply(DTO t);

        void goodbye();
    }

    static class DTO extends SelfDescribingMarshallable {
        String text;

        public DTO(String text) {
            this.text = text;
        }
    }

    static class PassMapService extends AbstractMapService<DTO, Reply> implements PassMapServiceIn {

        private transient DTO dataValue;

        @Override
        public void put(Bytes<?> key, DTO value) {
            map.put(key, value);
            reply.status(true);
        }

        @Override
        public void get(Bytes<?> key) {
            reply.reply(map.getUsing(key, dataValue()));
        }

        private DTO dataValue() {
            return dataValue == null ? dataValue = new DTO(null) : dataValue;
        }

        @Override
        public void remove(Bytes<?> key) {
            reply.status(map.remove(key, dataValue()));
        }

        @Override
        public Class<DTO> valueClass() {
            return DTO.class;
        }

        @Override
        public Class<Reply> replyClass() {
            return Reply.class;
        }

        @Override
        public void goodbye() {
            reply.goodbye();
            close();
        }
    }
}