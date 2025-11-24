/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.sg;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a field or method as a named stage within a compiled context.
 * <p>
 * The stage-compiler uses the {@link #value()} identifier to group related
 * fields and lifecycle methods, generating initialisation and teardown code
 * for them in the concrete context classes. At runtime the annotation is
 * available for tooling and diagnostics only.
 */
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Stage {
    String value();
}
