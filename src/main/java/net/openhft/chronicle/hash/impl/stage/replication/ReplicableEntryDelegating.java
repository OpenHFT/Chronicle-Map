/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.stage.replication;

import net.openhft.chronicle.hash.replication.ReplicableEntry;

/**
 * Convenience interface for {@link ReplicableEntry} implementations that delegate replication
 * metadata to another entry instance.
 *
 * <p>Implementors provide a single {@link #d()} method that exposes the underlying
 * {@link ReplicableEntry}; all replication-related methods are then forwarded by default. This
 * keeps wrapper and decorator types small while preserving a single source of truth for origin
 * identifiers, timestamps, and change flags.
 */
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
