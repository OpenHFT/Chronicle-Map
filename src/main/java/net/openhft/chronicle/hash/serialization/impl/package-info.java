//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

/**
 * The package contains many classes, implementing interfaces from the parent {@code
 * net.openhft.chronicle.hash.serialization} package, and shouldn't be used in the user code (in 90%
 * of cases). These classes are used automatically under the hood, primarily though {@link
 * net.openhft.chronicle.hash.serialization.impl.SerializationBuilder} and it's use in {@link
 * net.openhft.chronicle.map.ChronicleMapBuilder}. That is why these classes are put into the
 * separate package (this package) and excluded from the generated Javadocs, to reduce noise.
 * <p>
 * The remaining 5% of cases when these classes might be useful in the user code -- overriding
 * with different {@link
 * net.openhft.chronicle.hash.serialization.impl.InstanceCreatingMarshaller#createInstance()} logic,
 * when the default strategy (call of the default constructor) doesn't work.
 * <p>
 * Another remaining 5% of cases: configuration of complex marshallers with sub-marshallers, see
 * {@link net.openhft.chronicle.hash.serialization.ListMarshaller}, {@link
 * net.openhft.chronicle.hash.serialization.SetMarshaller} and {@link
 * net.openhft.chronicle.hash.serialization.MapMarshaller} for some examples.
 * <p>
 * Classes in this package are undocumented and not guaranteed to stay compatible between
 * Chronicle Map versions; refer to the source code and use on your own risk.
 */
package net.openhft.chronicle.hash.serialization.impl;