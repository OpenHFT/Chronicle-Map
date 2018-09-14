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

package eg;

import net.openhft.chronicle.values.Array;
import net.openhft.chronicle.values.MaxUtf8Length;

/**
 * Created by Vanitha on 12/5/2014.
 */
public interface TestInstrumentVOInterface {

    int getSizeOfInstrumentIDArray();

    void setSizeOfInstrumentIDArray(int sizeOfInstrumentIDArray);

    String getSymbol();

    void setSymbol(@MaxUtf8Length(20) String symbol);

    String getCurrencyCode();

    void setCurrencyCode(@MaxUtf8Length(4) String currencyCode);

    @Array(length = 2)
    void setInstrumentIDAt(int location, TestInstrumentIDVOInterface instrumentID);

    TestInstrumentIDVOInterface getInstrumentIDAt(int location);

    interface TestInstrumentIDVOInterface {

        String getIdSource();

        void setIdSource(@MaxUtf8Length(6) String idSource);

        String getSecurityId();

        void setSecurityId(@MaxUtf8Length(100) String securityId);

    }
}
