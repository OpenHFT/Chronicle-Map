/*
 *      Copyright (C) 2016 Roman Leventov
 *      Copyright (C) 2010 The Guava Authors
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
