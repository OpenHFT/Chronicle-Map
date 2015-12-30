/*
 *      Copyright (C) 2015  higherfrequencytrading.com
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
