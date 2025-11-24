/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.sg;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that a field should be injected with a reference to another stage.
 * <p>
 * When the stage-compiler generates a concrete context it wires fields marked
 * with {@code @StageRef} to the corresponding {@code @Stage} instances,
 * allowing stages to depend on each other without manual plumbing in user
 * code.
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface StageRef {
}
