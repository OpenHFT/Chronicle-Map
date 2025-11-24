/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
/**
 * Staged replication helpers for Chronicle-Hash.
 * <p>
 * This package contains staged components that adapt hash entries for
 * replication, delegating to higher-level replication strategies in
 * {@code net.openhft.chronicle.hash.replication}. They are used inside
 * Chronicle-Map and Chronicle-Set to integrate replication with the
 * staged execution model.
 */
package net.openhft.chronicle.hash.impl.stage.replication;
