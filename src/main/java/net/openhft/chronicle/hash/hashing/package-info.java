/*
 *      Copyright (C) 2015  higherfrequencytrading.com
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
 * API for hashing sequential data and zero-allocation, pretty fast implementations
 * of non-cryptographic hash functions.
 *
 * <p>Available implementations:
 * <ul>
 *     <li>{@code long}-valued functions: see
 *     {@link net.openhft.chronicle.hash.hashing.LongHashFunction}
 *     <ul>
 *         <li><a href="https://code.google.com/p/cityhash/">CityHash</a> 1.1:
 *         {@linkplain net.openhft.chronicle.hash.hashing.LongHashFunction#city_1_1() without 
 *         seeds},
 *         {@linkplain net.openhft.chronicle.hash.hashing.LongHashFunction#city_1_1(long)
 *         with one seed},
 *         {@linkplain net.openhft.chronicle.hash.hashing.LongHashFunction#city_1_1(long, long)
 *         with two seeds}.
 *         </li>
 *     </ul>
 *     </li>
 * </ul>
 */
package net.openhft.chronicle.hash.hashing;