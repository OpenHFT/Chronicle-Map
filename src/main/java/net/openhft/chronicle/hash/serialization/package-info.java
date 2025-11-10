//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

/**
 * Contains interfaces for serializing objects between Java heap and {@link
 * net.openhft.chronicle.bytes.Bytes} or {@link net.openhft.chronicle.bytes.BytesStore}, used by
 * Chronicle Map to store objects off-heap, and read them back from off-heap memory to on-heap
 * objects.
 * <p>
 * Read <a href="https://github.com/OpenHFT/Chronicle-Map/blob/ea/docs/CM_Tutorial.adoc#custom-serializers">Custom serializers
 * </a> section in the Chronicle Map tutorial for more information.
 * <p>
 * Reading methods in the interfaces in this package could use {@link
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