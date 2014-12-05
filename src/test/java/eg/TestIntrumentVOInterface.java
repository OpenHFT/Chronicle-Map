package eg;

import net.openhft.lang.model.constraints.MaxSize;

/**
 * Created by Vanitha on 12/5/2014.
 */
public interface TestIntrumentVOInterface {


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
