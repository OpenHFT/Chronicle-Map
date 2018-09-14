/*
 * Copyright 2012-2018 Chronicle Map Contributors
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

/**
 * The package contains many classes, implementing interfaces from the parent {@code
 * net.openhft.chronicle.hash.serialization} package, and shouldn't be used in the user code (in 90%
 * of cases). These classes are used automatically under the hood, primarily though {@link
 * net.openhft.chronicle.hash.serialization.impl.SerializationBuilder} and it's use in {@link
 * net.openhft.chronicle.map.ChronicleMapBuilder}. That is why these classes are put into the
 * separate package (this package) and excluded from the generated Javadocs, to reduce noise.
 * <p>
 * <p>The remaining 5% of cases when these classes might be useful in the user code -- overriding
 * with different {@link
 * net.openhft.chronicle.hash.serialization.impl.InstanceCreatingMarshaller#createInstance()} logic,
 * when the default strategy (call of the default constructor) doesn't work.
 * <p>
 * <p>Another remaining 5% of cases: configuration of complex marshallers with sub-marshallers, see
 * {@link net.openhft.chronicle.hash.serialization.ListMarshaller}, {@link
 * net.openhft.chronicle.hash.serialization.SetMarshaller} and {@link
 * net.openhft.chronicle.hash.serialization.MapMarshaller} for some examples.
 * <p>
 * <p>Classes in this package are undocumented and not guaranteed to stay compatible between
 * Chronicle Map versions; refer to the source code and use on your own risk.
 */
package net.openhft.chronicle.hash.serialization.impl;