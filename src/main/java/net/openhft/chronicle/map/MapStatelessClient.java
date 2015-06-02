package net.openhft.chronicle.map;


import net.openhft.chronicle.network.connection.AbstactStatelessClient;
import net.openhft.chronicle.network.connection.ClientWiredStatelessTcpConnectionHub;
import net.openhft.chronicle.wire.CoreFields;
import net.openhft.chronicle.wire.ParameterizeWireKey;
import net.openhft.chronicle.wire.ValueIn;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;

/**
 * Created by Rob Austin
 */
public abstract class MapStatelessClient<E extends ParameterizeWireKey> extends AbstactStatelessClient<E> {

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
            R usingValue,
            @NotNull final Class<R> resultType,
            @Nullable Object... args) {

        Function<ValueIn, R> consumerIn = resultType == CharSequence.class && usingValue != null
                ? f -> {
            f.textTo((StringBuilder) usingValue);
            return usingValue;
        }
                : f -> f.object(resultType);
        return proxyReturnWireConsumerInOut(eventId,
                CoreFields.reply,
                toParameters(eventId, args),
                consumerIn);
    }
}
