package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.ChronicleHashInstanceBuilder;
import net.openhft.chronicle.hash.replication.ReplicationHub;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.SECONDS;
import static net.openhft.chronicle.map.ChronicleMapBuilder.of;

/**
 * Created by Rob Austin
 */
public class MapWireConnectionHub<K, V> implements Cloneable{

    private static final Logger LOG = LoggerFactory.getLogger(MapWireHandler.class);
    public static final int MAP_SERVICE = 3;


    protected ChronicleMap<String, Integer> channelNameToId;
    private Supplier<ChronicleHashInstanceBuilder<ChronicleMap<K, V>>> mapFactory;
    private final Map<Integer, Replica> channelMap;
    private final ReplicationHub hub;

    private final ArrayList<BytesChronicleMap> bytesChronicleMaps = new ArrayList<>();
    private final ChannelProvider provider;


    public MapWireConnectionHub(
            @NotNull final Supplier<ChronicleHashInstanceBuilder<ChronicleMap<K, V>>> mapFactory,
            @NotNull final Supplier<ChronicleHashInstanceBuilder<ChronicleMap<String, Integer>>>
                    channelNameToIdFactory,
            byte localIdentifier,
            int serverPort) throws IOException {


        this.mapFactory = mapFactory;

        final TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
                .of(serverPort)
                .heartBeatInterval(1, SECONDS);

        hub = ReplicationHub.builder().tcpTransportAndNetwork(tcpConfig).createWithId(localIdentifier);

        channelNameToId = (ChronicleMap) channelNameToIdFactory.get()
                .replicatedViaChannel(hub.createChannel(MAP_SERVICE)).create();

        provider = ChannelProvider.getProvider(hub);
        channelMap = provider.chronicleChannelMap();

    }


    /**
     * @return the next free channel id
     */
    short getNextFreeChannel() {

        // todo this is a horrible hack, it slow and not processor safe, but was added to get
        // todo something working for now.

        int max = 3;
        for (Integer channel : channelNameToId.values()) {
            max = Math.max(max, channel);
        }

        return (short) (max + 1);
    }

    /**
     * gets the channel id for a name, or creates a new one if this name is not yet assosiated to a
     * channel
     *
     * @param fromName the name of the channel
     * @return the id associated with this name
     */
    BytesChronicleMap acquireMap(@NotNull final String fromName) throws IOException {

        // todo this is a horrible hack, it slow and NOT processor safe, but was added to get
        // todo something working for now.

        final Integer channelId = channelNameToId.get(fromName);

        if (channelId != null)
            return bytesMap(channelId);

        final int nextFreeChannel = getNextFreeChannel();
        try {

            mapFactory.get().replicatedViaChannel(hub.createChannel(nextFreeChannel)).create();
            channelNameToId.put(fromName, nextFreeChannel);

            return bytesMap(nextFreeChannel);
        } catch (IOException e) {
            // todo send this error back to the user
            LOG.error("", e);
            throw e;
        }



    }


    /**
     * this is used to push the data straight into the entry in memory
     *
     * @param channelId the ID of the map
     * @return a BytesChronicleMap used to update the memory which holds the chronicle map
     */
    @Nullable
    BytesChronicleMap bytesMap(int channelId) {

        final BytesChronicleMap bytesChronicleMap = (channelId < bytesChronicleMaps.size())
                ? bytesChronicleMaps.get(channelId)
                : null;

        if (bytesChronicleMap != null)
            return bytesChronicleMap;

        // grow the array
        for (int i = bytesChronicleMaps.size(); i <= channelId; i++) {
            bytesChronicleMaps.add(null);
        }

        final ReplicatedChronicleMap delegate = map(channelId);
        final BytesChronicleMap element = new BytesChronicleMap(delegate);
        bytesChronicleMaps.set(channelId, element);
        return element;

    }

    /**
     * gets the map for this channel id
     *
     * @param channelId the ID of the map
     * @return the chronicle map with this {@code channelId}
     */
    @NotNull
    private ReplicatedChronicleMap map(int channelId) {

        // todo this cast is a bit of a hack, improve later
        final ReplicatedChronicleMap map =
                (ReplicatedChronicleMap) channelMap.get(channelId);

        if (map != null)
            return map;

        throw new IllegalStateException();
    }

    public void close() throws IOException {
        provider.close();
    }
}
