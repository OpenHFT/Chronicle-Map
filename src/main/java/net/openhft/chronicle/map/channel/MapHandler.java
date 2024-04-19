package net.openhft.chronicle.map.channel;

import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.channel.internal.MapChannel;
import net.openhft.chronicle.wire.channel.AbstractHandler;
import net.openhft.chronicle.wire.channel.ChronicleChannel;
import net.openhft.chronicle.wire.channel.ChronicleChannelCfg;
import net.openhft.chronicle.wire.channel.ChronicleContext;

import java.io.IOException;

@SuppressWarnings({"rawtypes", "unchecked"})
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
        try (ChronicleMap<Bytes<?>, VALUE> map = MapChannel.createMap(mapName, mapService, context)) {
            REPLY REPLY = channel.methodWriter(mapService().replyClass());
            mapService.map(map);
            mapService.reply(REPLY);
            try (AffinityLock lock = context.affinityLock()) {
                assert lock != null;
                channel.eventHandlerAsRunnable(mapService).run();
            }

        } catch (IOException ioe) {
            throw Jvm.rethrow(ioe);
        }
    }

    @Override
    public ChronicleChannel asInternalChannel(ChronicleContext context, ChronicleChannelCfg channelCfg) {
        return new MapChannel(mapName, mapService, context, channelCfg);
    }

    protected MapService<VALUE, REPLY> mapService() {
        return mapService;
    }
}
