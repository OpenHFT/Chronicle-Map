/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.impl;

import net.openhft.sg.Staged;

/**
 * Marker stage used as an anchor for staged Chronicle-Map contexts.
 *
 * <p>Provides a stable root in the generated context class hierarchy so that additional
 * stages can be attached without changing user-visible types.
 */
@Staged
public class CompilationAnchor {
}
