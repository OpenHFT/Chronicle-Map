/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */

package net.openhft.chronicle.hash;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;

/**
 * Signifies that a public API (public class, method or field) is subject to incompatible changes,
 * or even removal, in a future release. An API bearing this annotation is exempt from any
 * compatibility guarantees. Note that the presence of this annotation implies nothing about the
 * quality or performance of the API in question, only the fact that it is not "API-frozen."
 * <p>
 * This annotation is a copy of Guava's <a href="https://google.github.io/guava/releases/19.0/api/docs/com/google/common/annotations/Beta.html">
 * {@code Beta}</a>.
 */
@Retention(value = RetentionPolicy.CLASS)
@Target(value = {ANNOTATION_TYPE, CONSTRUCTOR, FIELD, METHOD, TYPE})
@Documented
@Beta
public @interface Beta {
}
