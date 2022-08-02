package net.openhft.chronicle.map.channel;

import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.hash.serialization.impl.BytesMarshallableReaderWriter;
import net.openhft.chronicle.hash.serialization.impl.BytesSizedMarshaller;
import net.openhft.chronicle.hash.serialization.impl.MarshallableReaderWriter;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.channel.AbstractHandler;
import net.openhft.chronicle.wire.channel.ChronicleChannel;
import net.openhft.chronicle.wire.channel.ChronicleChannelCfg;
import net.openhft.chronicle.wire.channel.ChronicleContext;

import java.io.IOException;

public class MapHandler<VALUE, REPLY> extends AbstractHandler<MapHandler<VALUE, REPLY>> {
    protected MapService<VALUE, REPLY> mapService;
    private String mapName;

    protected MapHandler(String mapName) {
        this.mapName = mapName;
    }

    public static <V, O> MapHandler<V, O> createMapHandler(String mapName, MapService<V, O> mapService) {
        MapHandler<V, O> mh = new MapHandler<>(mapName);
        mh.mapService = mapService;
        return mh;
    }

    @Override
    public void run(ChronicleContext context, ChronicleChannel channel) {
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
        try (ChronicleMap<Bytes<?>, VALUE> map = builder
                .createPersistedTo(context.toFile(mapName + ".cm3"))) {
            REPLY REPLY = channel.methodWriter(mapService().replyClass());
            mapService.map(map);
            mapService.reply(REPLY);
            try (AffinityLock lock = context.affinityLock()) {
                channel.eventHandlerAsRunnable(mapService).run();
            }

        } catch (IOException ioe) {
            throw Jvm.rethrow(ioe);
        }
    }

    @Override
    public ChronicleChannel asInternalChannel(ChronicleContext context, ChronicleChannelCfg channelCfg) {
        throw new UnsupportedOperationException();
    }

    protected MapService<VALUE, REPLY> mapService() {
        return mapService;
    }
}
