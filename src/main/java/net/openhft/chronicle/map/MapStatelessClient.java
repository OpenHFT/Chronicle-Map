package net.openhft.chronicle.map;

import net.openhft.chronicle.engine.client.ClientWiredStatelessTcpConnectionHub;
import net.openhft.chronicle.wire.CoreFields;
import net.openhft.chronicle.wire.ParameterizeWireKey;
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
                CoreFields.reply, toParameters(eventId, args),
                f -> f.object(resultType));

    }


}
