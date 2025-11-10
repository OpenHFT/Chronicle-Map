//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

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
