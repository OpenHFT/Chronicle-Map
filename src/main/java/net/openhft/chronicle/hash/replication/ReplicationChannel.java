/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.hash.replication;

import net.openhft.chronicle.hash.ChronicleHash;
import net.openhft.chronicle.hash.ChronicleHashInstanceBuilder;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;

/**
 * A token which should be created via {@link ReplicationHub#createChannel(int)} call and passed to {@link
 * ChronicleHashInstanceBuilder#replicatedViaChannel(ReplicationChannel)} method, to establish a channel
 * replication. See <a href="https://github.com/OpenHFT/Chronicle-Map#multiple-chronicle-maps---network-distributed">
 * the corresponding section in ChronicleMap manual</a> for more information.
 *
 * <p>A {@code ReplicationChannel} could be used to replicate only one {@link ChronicleHash} instance.
 *
 * @see ReplicationHub
 * @see ReplicationHub#createChannel(int)
 */
public final class ReplicationChannel implements Serializable{
    private ReplicationHub hub;
    private final int channelId;

    ReplicationChannel(ReplicationHub hub, int channelId) {
        this.hub = hub;
        this.channelId = channelId;
    }

    /**
     * Returns the {@link ReplicationHub} on which this {@code ReplicationChannel} was {@linkplain
     * ReplicationHub#createChannel(int) created}.
     *
     * @return the {@code ReplicationHub} to which this {@code ReplicationChannel} belongs
     * @see ReplicationHub#createChannel(int)
     */
    @NotNull
    public ReplicationHub hub() {
        return hub;
    }

    /**
     * Returns the identifier of this channel, with which was {@linkplain ReplicationHub#createChannel(int)
     * created}.
     *
     * @return the identifier of this channel
     * @see ReplicationHub#createChannel(int)
     */
    public int channelId() {
        return channelId;
    }
}
