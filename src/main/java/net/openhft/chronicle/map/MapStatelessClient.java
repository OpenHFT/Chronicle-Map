package net.openhft.chronicle.map;

import net.openhft.chronicle.engine.client.ClientWiredStatelessTcpConnectionHub;
import net.openhft.chronicle.engine.client.ParameterizeWireKey;
import net.openhft.chronicle.wire.WireKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Created by Rob Austin
 */
public abstract class MapStatelessClient< E extends ParameterizeWireKey> extends AbstactStatelessClient<E> {

    /**
     * @param channelName
     * @param hub
     * @param type        the type of wire handler for example "MAP" or "QUEUE"
     * @param cid         used by proxies such as the entry-set
     */
    public MapStatelessClient(@NotNull String channelName,
                              @NotNull ClientWiredStatelessTcpConnectionHub hub,
                              @NotNull String type,
                              long cid) {
        super(channelName, hub, type, cid);

    }

    @Nullable
    protected <R> R proxyReturnTypedObject(
            @NotNull final E eventId,
            @NotNull final Class<R> resultType,
            @Nullable Object... args) {

        return proxyReturnWireConsumerInOut(eventId,
                toParameters(eventId, args),
                f -> f.read(ClientWiredStatelessTcpConnectionHub.CoreFields.reply)
                        .object(resultType));

    }




    protected <E, O> E proxyReturnE(@NotNull final WireKey eventId,
                                    @Nullable final O arg,
                                    @NotNull final Class<E> eClass) {


        return proxyReturnWireConsumerInOut(eventId,
                valueOut -> valueOut.object(arg),
                f -> f.read(ClientWiredStatelessTcpConnectionHub.CoreFields.reply)
                        .object(eClass));
    }

    protected <E> E proxyReturnE(@NotNull final WireKey eventId,
                                 @NotNull final Class<E> eClass) {
        return proxyReturnWireConsumer(eventId,
                f -> f.read(ClientWiredStatelessTcpConnectionHub.CoreFields.reply)
                .object(eClass));
    }


}
