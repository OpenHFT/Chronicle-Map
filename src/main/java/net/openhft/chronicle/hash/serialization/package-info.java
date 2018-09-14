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