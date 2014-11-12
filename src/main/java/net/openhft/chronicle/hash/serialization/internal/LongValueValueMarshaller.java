/*
 * Copyright 2014 Higher Frequency Trading http://www.higherfrequencytrading.com
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

package net.openhft.chronicle.hash.serialization.internal;

import net.openhft.lang.io.Bytes;
import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.values.LongValue;

public final class LongValueValueMarshaller extends LongValueKeyMarshaller {
    private static final long serialVersionUID = 0L;

    public static final LongValueValueMarshaller INSTANCE = new LongValueValueMarshaller();

    private static final Class<LongValue> directLongValueClass =
            DataValueClasses.directClassFor(LongValue.class);

    private LongValueValueMarshaller() {}

    private Object readResolve() {
        return INSTANCE;
    }

    @Override
    public LongValue read(Bytes bytes, long size, LongValue toReuse) {
        if (!(toReuse instanceof Byteable)) {
            try {
                toReuse = directLongValueClass.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                throw new AssertionError(e);
            }
        }
        Byteable value = (Byteable) toReuse;
        ByteableMarshaller.setBytesAndOffset(value, bytes);
        bytes.skip(8L);
        return toReuse;
    }
}
