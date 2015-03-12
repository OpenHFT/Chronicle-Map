/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map;

import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;

public class ServiceDescriptor<K, V> implements Marshallable {

        public Class<K> keyClass;
        public Class<V> valueClass;
        public short channelID;

    public ServiceDescriptor() {
    }

    public ServiceDescriptor(Class<K> keyClass, Class<V> valueClass, short channelID) {
            this.keyClass = keyClass;
            this.valueClass = valueClass;
            this.channelID = channelID;
        }


        @Override
        public void writeMarshallable(WireOut wire) {
            wire.write(() -> "keyClass").text(keyClass.getName());
            wire.write(() -> "valueClass").text(valueClass.getName());
            wire.write(() -> "channelID").int16(channelID);
        }

        @Override
        public void readMarshallable(WireIn wire) throws IllegalStateException {
            try {
                keyClass = (Class<K>) Class.forName(wire.read(() -> "keyClass").text());
                valueClass = (Class<V>) Class.forName(wire.read(() -> "valueClass").text());
                channelID = wire.read(() -> "channelID").int16();
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ServiceDescriptor serviceDescriptor = (ServiceDescriptor) o;

            if (channelID != serviceDescriptor.channelID) return false;
            if (!keyClass.equals(serviceDescriptor.keyClass)) return false;
            if (!valueClass.equals(serviceDescriptor.valueClass)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = keyClass.hashCode();
            result = 31 * result + valueClass.hashCode();
            result = 31 * result + (int) channelID;
            return result;
        }
    }