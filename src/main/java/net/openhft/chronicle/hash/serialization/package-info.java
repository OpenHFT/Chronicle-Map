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
 * Contains interfaces for serializing objects between Java heap and {@link
 * net.openhft.chronicle.bytes.Bytes} or {@link net.openhft.chronicle.bytes.BytesStore}, used by
 * Chronicle Map to store objects off-heap, and read them back from off-heap memory to on-heap
 * objects.
 * <p>
 * <p>Read <a href="https://github.com/OpenHFT/Chronicle-Map#custom-serializers">Custom serializers
 * </a> section in the Chronicle Map tutorial for more information.
 * <p>
 * <p>Reading methods in the interfaces in this package could use {@link
 * net.openhft.chronicle.bytes.StreamingDataInput} as the "input" parameter type and {@link
 * net.openhft.chronicle.bytes.StreamingDataOutput} as the "output" parameter type, but always use
 * just {@link net.openhft.chronicle.bytes.Bytes} and {@link net.openhft.chronicle.bytes.BytesStore
 * }, though this is "unsafe" because somebody could make a mistake and write into bytes supposed to
 * be read-only or read some garbage bytes. This is done because of the poor support of
 * StreamingDataInput/StreamingDataOutput in Chronicle Bytes and other projects. {@link
 * net.openhft.chronicle.bytes.Byteable}, {@link net.openhft.chronicle.bytes.BytesMarshallable},
 * Chronicle Wire and others support only BytesStore or Bytes, but not read/write separated
 * interfaces.
 */
package net.openhft.chronicle.hash.serialization;