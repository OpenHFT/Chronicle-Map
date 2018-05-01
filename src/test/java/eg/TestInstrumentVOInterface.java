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
