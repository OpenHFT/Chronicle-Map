package net.openhft.chronicle.map.channel.internal;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.SimpleCloseable;
import net.openhft.chronicle.hash.serialization.impl.BytesMarshallableReaderWriter;
import net.openhft.chronicle.hash.serialization.impl.BytesSizedMarshaller;
import net.openhft.chronicle.hash.serialization.impl.MarshallableReaderWriter;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.map.channel.MapService;
import net.openhft.chronicle.wire.*;
import net.openhft.chronicle.wire.channel.*;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

@SuppressWarnings({"rawtypes", "unchecked"})
public class MapChannel<VALUE, REPLY> extends SimpleCloseable implements ChronicleChannel {

    private static final OkHeader OK = new OkHeader();
    private final String mapName;
    private final MapService<VALUE, REPLY> mapService;
    private final ChronicleChannelCfg channelCfg;
    private final ChronicleMap<Bytes<?>, VALUE> map;
    // TODO FIX this runs out of memory.
    private final Wire replyWire = WireType.BINARY_LIGHT.apply(Bytes.allocateElasticOnHeap());

    public MapChannel(String mapName, MapService<VALUE, REPLY> mapService, ChronicleContext context, ChronicleChannelCfg channelCfg) {
        this.mapName = mapName;
        this.mapService = mapService;
        this.channelCfg = channelCfg;
        try {
            map = createMap(mapName, mapService, context);
            mapService.map(map);
            REPLY reply = replyWire.methodWriter(mapService.replyClass());
            mapService.reply(reply);
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }
    }

    public static <VALUE, REPLY> ChronicleMap<Bytes<?>, VALUE> createMap(String mapName, MapService<VALUE, REPLY> mapService, ChronicleContext context) throws IOException {
        // assume it has to already exist, but if not take a guess on sizes
        final Class<VALUE> valueClass = mapService.valueClass();
        final Class<Bytes<?>> bytesClass = (Class) Bytes.class;
        final ChronicleMapBuilder<Bytes<?>, VALUE> builder = ChronicleMap.of(bytesClass, valueClass)
                .keyMarshaller(new BytesSizedMarshaller())
                .averageKeySize(32)
                .averageValueSize(256)
                .entries(1000000)
                .putReturnsNull(true)
                .removeReturnsNull(true);
        if (BytesMarshallable.class.isAssignableFrom(valueClass)) {
            //noinspection unchecked,rawtypes
            builder.valueMarshaller(new BytesMarshallableReaderWriter<>((Class) valueClass));
        } else if (Marshallable.class.isAssignableFrom(valueClass)) {
            //noinspection unchecked,rawtypes
            builder.valueMarshaller(new MarshallableReaderWriter<>((Class) valueClass));
        }
        return builder
                .createPersistedTo(context.toFile(mapName + ".cm3"));
    }

    @Override
    public ChronicleChannelCfg channelCfg() {
        return channelCfg;
    }

    @Override
    public ChannelHeader headerOut() {
        return OK;
    }

    @Override
    public ChannelHeader headerIn() {
        return OK;
    }

    @Override
    public void testMessage(long now) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long lastTestMessage() {
        throw new UnsupportedOperationException();
    }

    @Override
    public @NotNull DocumentContext readingDocument() {
        return replyWire.readingDocument();
    }

    @Override
    public <T> @NotNull T methodWriter(@NotNull Class<T> tClass, Class<?>... additional) {
        // if the class doesn't match it throws a ClassCastException'
        return (T) mapService;
    }

    @Override
    public DocumentContext writingDocument(boolean metaData) throws UnrecoverableTimeoutException {
        throw new UnsupportedOperationException();
    }

    @Override
    public DocumentContext acquireWritingDocument(boolean metaData) throws UnrecoverableTimeoutException {
        throw new UnsupportedOperationException();
    }
}
