/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.sg;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a type as participating in the Chronicle staging model.
 * <p>
 * Classes and interfaces annotated with {@code @Staged} are candidates for
 * inclusion in {@link Context}-annotated pipelines; the stage-compiler may
 * generate specialised implementations or context wrappers for them. The
 * annotation is inherited so that subclasses automatically take part in the
 * same staging scheme.
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface Staged {
}
