/*
 * Copyright 2012-2018 Chronicle Map Contributors
 * Copyright 2010 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
 * <p>This annotation is a copy of Guava's <a href="
 * https://google.github.io/guava/releases/19.0/api/docs/com/google/common/annotations/Beta.html">
 * {@code Beta}</a>.
 */
@Retention(value = RetentionPolicy.CLASS)
@Target(value = {ANNOTATION_TYPE, CONSTRUCTOR, FIELD, METHOD, TYPE})
@Documented
@Beta
public @interface Beta {
}
