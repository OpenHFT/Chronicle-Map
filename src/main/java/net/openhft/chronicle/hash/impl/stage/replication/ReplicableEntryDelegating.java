/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.hash.impl.stage.replication;

import net.openhft.chronicle.hash.replication.ReplicableEntry;

public interface ReplicableEntryDelegating extends ReplicableEntry {

    ReplicableEntry d();

    @Override
    default byte originIdentifier() {
        return d().originIdentifier();
    }

    @Override
    default long originTimestamp() {
        return d().originTimestamp();
    }

    @Override
    default void updateOrigin(byte newIdentifier, long newTimestamp) {
        d().updateOrigin(newIdentifier, newTimestamp);
    }

    @Override
    default void dropChanged() {
        d().dropChanged();
    }

    @Override
    default void dropChangedFor(byte remoteIdentifier) {
        d().dropChangedFor(remoteIdentifier);
    }

    @Override
    default void raiseChanged() {
        d().raiseChanged();
    }

    @Override
    default void raiseChangedFor(byte remoteIdentifier) {
        d().raiseChangedFor(remoteIdentifier);
    }

    @Override
    default void raiseChangedForAllExcept(byte remoteIdentifier) {
        d().raiseChangedForAllExcept(remoteIdentifier);
    }

    @Override
    default boolean isChanged() {
        return d().isChanged();
    }

    @Override
    default void doRemoveCompletely() {
        d().doRemoveCompletely();
    }
}
