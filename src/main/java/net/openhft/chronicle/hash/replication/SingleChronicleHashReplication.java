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

import org.jetbrains.annotations.NotNull;

public class SingleChronicleHashReplication extends AbstractReplication {

    @NotNull
    public static Builder builder() {
        return new Builder();
    }

    public SingleChronicleHashReplication(byte localIdentifier, Builder builder) {
        super(localIdentifier, builder);
    }

    @Override
    public String toString() {
        return "SingleChronicleHashReplication{" + super.toString() + "}";
    }

    public static class Builder
            extends AbstractReplication.Builder<SingleChronicleHashReplication, Builder> {
        private Builder() {}

        @NotNull
        @Override
        public SingleChronicleHashReplication createWithId(byte identifier) {
            check(identifier);
            return new SingleChronicleHashReplication(identifier, this);
        }
    }
}
