/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
/**
 * Annotations and support types for the Chronicle staged execution model.
 * <p>
 * The {@code @Staged} and {@code @StageRef} annotations and related
 * helpers describe how Chronicle-Map and Chronicle-Set implementations
 * are decomposed into reusable stages. Code generation tools consume
 * these annotations to assemble efficient pipelines.
 */
package net.openhft.sg;
