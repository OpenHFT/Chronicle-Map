/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
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