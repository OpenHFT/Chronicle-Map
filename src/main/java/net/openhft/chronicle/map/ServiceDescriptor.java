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