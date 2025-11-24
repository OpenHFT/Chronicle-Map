/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.sg;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares the set of staged components that form a compiled context.
 * <p>
 * The stage-compiler processes types annotated with {@code @Context} and
 * generates concrete context classes wiring together the {@code topLevel} and
 * {@code nested} stage classes. The annotation is retained in source only and
 * is intended solely for the Chronicle staging toolchain.
 */
@Retention(RetentionPolicy.SOURCE)
@Target({ElementType.TYPE})
public @interface Context {
    Class<?>[] topLevel();

    Class<?>[] nested();
}
