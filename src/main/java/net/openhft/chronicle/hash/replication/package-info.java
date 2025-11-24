/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
/**
 * Core replication contracts for Chronicle-Hash.
 * <p>
 * The classes and interfaces in this package model how hash entries
 * participate in eventual-consistency protocols across nodes. They
 * define common abstractions such as {@code ReplicableEntry},
 * timestamps and node identifiers together with default strategies
 * like {@code DefaultEventualConsistencyStrategy}.
 * <p>
 * Higher-level modules such as Chronicle-Map and Chronicle-Set build
 * on these contracts to implement replicated maps and sets.
 */
package net.openhft.chronicle.hash.replication;

