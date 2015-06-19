/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package eg;

import net.openhft.lang.model.constraints.MaxSize;

/**
 * Created by Vanitha on 12/5/2014.
 */
public interface TestInstrumentVOInterface {


    public int getSizeOfInstrumentIDArray();

    public void setSizeOfInstrumentIDArray(int sizeOfInstrumentIDArray);


    public String getSymbol();

    public void setSymbol(@MaxSize(20) String symbol);




    public String getCurrencyCode();

    public void setCurrencyCode(@MaxSize(4) String currencyCode);


    public void setInstrumentIDAt(@MaxSize(2) int location, TestInstrumentIDVOInterface instrumentID);

    public TestInstrumentIDVOInterface getInstrumentIDAt(int location);


    interface TestInstrumentIDVOInterface {


        public String getIdSource();

        public void setIdSource(@MaxSize(6) String idSource);

        public String getSecurityId();

        public void setSecurityId(@MaxSize(100) String securityId);

    }
}
