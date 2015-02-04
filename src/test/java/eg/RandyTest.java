package eg;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.map.ChronicleMapStatelessClientBuilder;
import org.junit.Test;

import java.net.InetSocketAddress;

/**
 * @author Rob Austin.
 */
public class RandyTest {

    @Test
    public void testName() throws Exception {


        ChronicleMapStatelessClientBuilder<CharSequence, InstrumentVOInterface> builder =
                ChronicleMapBuilder.of(CharSequence.class, InstrumentVOInterface.class, new
                        InetSocketAddress("localhost", 8090));
        //ChronicleMapBuilder builder = ChronicleMapBuilder.of(SmallCharSequence.class, InstrumentVOInterface.class)
        builder
                .putReturnsNull(true)
                .removeReturnsNull(true);
        //TODO: not used for stateless client
        //.entries(numberOfEntries)

        //
        // TODO use the stateless client for now
        //.replication((byte) 2, tcpTransportAndNetworkConfig)

        //
        // TODO It looks like the stateLess client skips any event listener processing
        //.eventListener(new MapEventListener<SmallCharSequence, InstrumentVOInterface>() {
        //    @Override
        //    public void onPut(SmallCharSequence key, InstrumentVOInterface newValue, InstrumentVOInterface replacedValue) {
        //        //
        //        //TODO make this a trace operation once fully baked
        //        if (log.isDebugEnabled()) {
        //            log.debug("onPut Instrument Key: " + key);
        //        }
        //    }
        //}
        //);

        //
        // TODO We are using the replicated client
        ChronicleMap<CharSequence, InstrumentVOInterface> charSequenceInstrumentVOInterfaceChronicleMap = builder.create();


    }
}
