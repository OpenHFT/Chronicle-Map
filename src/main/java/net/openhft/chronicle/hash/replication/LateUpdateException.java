/*
 * Copyright 2014 Higher Frequency Trading http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.hash.replication;

/**
 * Thrown if update to replicated {@link net.openhft.chronicle.hash.ChronicleHash} is rejected
 * due to local time is lesser than the time of the latest update to the entry. This most likely
 * indicates bugs or largely inconsistent timing across replicating nodes. If this is tolerated,
 * you should catch {@code LateUpdateException} and ignore it.
 */
public class LateUpdateException extends RuntimeException {
    private static final long serialVersionUID = 0L;

    private final byte originIdentifier;
    private final long lastUpdateTimestamp;
    private final byte thisNodeIdentifier;
    private final long thisUpdateTimestamp;

    public LateUpdateException(byte originIdentifier, long lastUpdateTimestamp,
                               byte thisNodeIdentifier, long thisUpdateTimestamp) {
        super("Late update : " +
                "last update time: " + lastUpdateTimestamp +
                ", last update origin node identifier: " + originIdentifier +
                ", time of this update: " + thisUpdateTimestamp +
                ", identifier of this node: " + thisNodeIdentifier);
        this.originIdentifier = originIdentifier;
        this.lastUpdateTimestamp = lastUpdateTimestamp;
        this.thisNodeIdentifier = thisNodeIdentifier;
        this.thisUpdateTimestamp = thisUpdateTimestamp;
    }

    public long thisUpdateTimestamp() {
        return thisUpdateTimestamp;
    }

    public byte thisNodeIdentifier() {
        return thisNodeIdentifier;
    }

    public long lastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public byte lastUpdateOriginNodeIdentifier() {
        return originIdentifier;
    }
}
