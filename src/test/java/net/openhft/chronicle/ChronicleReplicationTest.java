package net.openhft.chronicle;

import net.openhft.chronicle.hash.replication.ConnectionListener;
import net.openhft.chronicle.hash.replication.ReplicationHub;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.map.MapEventListener;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public class ChronicleReplicationTest {


    private static final Logger LOG = LoggerFactory.getLogger(ChronicleReplicationTest.class);

    static Logger logger;
    static final long ENTRIES = 100_000;
    static final int ADD_ENTRIES = 10_000;
    static final int SLEEP_ADD_ENTRIES = 1;

    static final int MAP_ENTRIES_CHANNEL_ID = 1;
    static final int MAP_CHANNEL_IDENTIFIERS_CHANNEL_ID = 2;
    static final int ORDERS_CHANNEL_ID = 3;
    static final int ORDERS2_CHANNEL_ID = 4;
    static final int ORDERS3_CHANNEL_ID = 5;
    static final int ORDERS4_CHANNEL_ID = 6;


    @Test
    public void test() throws Exception {


        // Logback via SLF4
        String logbackFilePath = "./conf/logback.xml";

   /*     try {
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(context);
            context.reset();
            configurator.doConfigure(logbackFilePath);
        } catch (JoranException je) {
            // StatusPrinter will handle this
        }*/

        logger = LoggerFactory.getLogger(ChronicleReplicationTest.class);

        // Key/Value
        String fix = "8=FIXT.1.1|9=148|35=D|49=VERIFIX50|56=VALDI_BUS|34=SEQ|52=20141205-21:46:51.423|11=Order198|1=TestAccount|55=IPN|54=1|60=20141205-21:46:51.423|38=1000|40=2|44=90|59=0|10=028|";
        fix = fix.replace('|', '\001');

        String host = "10.215.11.250";
        int masterPort = 5000;
        int slavePort = 5001;
        int heartbeatInterval = 1;
        byte masterIdentifier = 1;
        byte slaveIdentifier = 2;

	    /* Master */
        TcpTransportAndNetworkConfig masterTcpReplicationConfig = TcpTransportAndNetworkConfig
                .of(masterPort, new InetSocketAddress(host, slavePort))
                .heartBeatInterval(heartbeatInterval, TimeUnit.SECONDS)
                .autoReconnectedUponDroppedConnection(true);

        ReplicationHub masterReplicationHub = ReplicationHub.builder()
                .tcpTransportAndNetwork(masterTcpReplicationConfig)
                .connectionListener(new ConnectionListener() {
                    @Override
                    public void onDisconnect(InetAddress address, byte identifier) {
                        logger.info("Master.onDisconnect(" + address + "," + identifier + ")");
                    }

                    @Override
                    public void onConnect(InetAddress address, byte identifier, boolean isServer) {
                        logger.info("Master.onConnect(" + address + "," + identifier + "," + isServer + ")");
                    }
                })
                .createWithId(masterIdentifier);

        MapContext master = new MapContext();
        Thread.sleep(10_000);
        createMap(master, masterReplicationHub, "master");
        Thread.sleep(2000);

		 /* Slave */
        TcpTransportAndNetworkConfig slaveTcpReplicationConfig = TcpTransportAndNetworkConfig
                .of(slavePort, new InetSocketAddress(host, masterPort))
                .heartBeatInterval(heartbeatInterval, TimeUnit.SECONDS)
                .autoReconnectedUponDroppedConnection(true);

        ReplicationHub slaveReplicationHub = ReplicationHub.builder()
                .tcpTransportAndNetwork(slaveTcpReplicationConfig)
                .connectionListener(new ConnectionListener() {
                    @Override
                    public void onDisconnect(InetAddress address, byte identifier) {
                        logger.info("Slave.onDisconnect(" + address + "," + identifier + ")");
                    }

                    @Override
                    public void onConnect(InetAddress address, byte identifier, boolean isServer) {
                        logger.info("Slave.onConnect(" + address + "," + identifier + "," + isServer + ")");
                    }
                })
                .createWithId(slaveIdentifier);
        MapContext slave = new MapContext();
        Thread.sleep(10_000);
        createMap(slave, slaveReplicationHub, "slave");
        Thread.sleep(2000);

		/* fill orders */
        logger.info("begin.add.orders " + master.orders.size());

        int index = master.orders.size();
        while (index < ADD_ENTRIES) {
            index++;
            Thread.sleep(SLEEP_ADD_ENTRIES);
            logger.info("add <" + index + ">");
            master.orders.put(index, fix.replace("SEQ", index + ""));
        }
        logger.info("end.add.orders " + master.orders.size() + " " + slave.orders.size());

        while (master.orders.size() != slave.orders.size()) {
            logger.info("Waiting for synchronization");
            Thread.sleep(1000);
        }

        while (true) {
            Thread.sleep(2000);
            logger.info("master orders " + master.orders.size());
            logger.info("slave orders " + slave.orders.size());
        }

    }

    private static void createMap(MapContext mapContext, ReplicationHub replicationHub, String path) throws Exception {
        logger.info("createMap for " + path);
        //     FileUtils.forceMkdir(new File("./store/test/" + path));

        mapContext.mapEntries = ChronicleMapBuilder.of(String.class, Long.class)
                .entries(ENTRIES)
                .instance()
                .replicatedViaChannel(replicationHub.createChannel(MAP_ENTRIES_CHANNEL_ID))
                .create();

        mapContext.mapChannelIdentifiers = ChronicleMapBuilder.of(String.class, Integer.class)
                .entries(ENTRIES)
                .instance()
                .replicatedViaChannel(replicationHub.createChannel(MAP_CHANNEL_IDENTIFIERS_CHANNEL_ID))
                .create();

        Thread.sleep(10_000);

        mapContext.orders2 = ChronicleMapBuilder.of(Integer.class, String.class)
                .entries(ENTRIES)
                .eventListener(new LoggerMapListener<Integer, String>("orders2"))
                .instance()
                .replicatedViaChannel(replicationHub.createChannel(ORDERS2_CHANNEL_ID))
                .persistedTo(new File("orders2"))
                .create();
        if (path.equals("master")) {
            mapContext.mapEntries.put("orders2", ENTRIES);
            mapContext.mapChannelIdentifiers.put("orders2", ORDERS2_CHANNEL_ID);
        }

        mapContext.orders = ChronicleMapBuilder.of(Integer.class, String.class)
                .entries(ENTRIES)
                .eventListener(new LoggerMapListener<Integer, String>("orders"))
                .instance()
                .replicatedViaChannel(replicationHub.createChannel(ORDERS_CHANNEL_ID))
                .persistedTo(new File("orders"))
                .create();
        if (path.equals("master")) {
            mapContext.mapEntries.put("orders", ENTRIES);
            mapContext.mapChannelIdentifiers.put("orders", ORDERS_CHANNEL_ID);
        }

        mapContext.orders3 = ChronicleMapBuilder.of(Integer.class, String.class)
                .entries(ENTRIES)
                .eventListener(new LoggerMapListener<Integer, String>("orders3"))
                .instance()
                .replicatedViaChannel(replicationHub.createChannel(ORDERS3_CHANNEL_ID))
                .persistedTo(new File("orders3"))
                .create();
        if (path.equals("master")) {
            mapContext.mapEntries.put("orders3", ENTRIES);
            mapContext.mapChannelIdentifiers.put("orders3", ORDERS3_CHANNEL_ID);
        }
    }

    private static final class MapContext {
        ChronicleMap<String, Long> mapEntries;
        ChronicleMap<String, Integer> mapChannelIdentifiers;
        ChronicleMap<Integer, String> orders;

        ChronicleMap<Integer, String> orders2;
        ChronicleMap<Integer, String> orders3;
        //  ChronicleMap<Integer, String> orders4;
    }

    private static final class LoggerMapListener<K, V> extends MapEventListener<K, V> {
        private static final long serialVersionUID = 1L;
        private final String id;

        public LoggerMapListener(String id) {
            this.id = id;
        }

        @Override
        public void onPut(K key, V newValue, V replacedValue, boolean replicationEvent, boolean added,
                          boolean hasValueChanged, byte identifier, byte replacedIdentifier, long timeStamp,
                          long replacedTimeStamp) {
            logger.info("<" + id + "> onPut(" + key + "," + getObjectValue(newValue) + "," + getObjectValue(replacedValue) + "," + replicationEvent + "," + added + "," + identifier + "," + replacedIdentifier + "," + timeStamp + "," + replacedTimeStamp + ")");
        }

        /**
         * Gets the object value.
         *
         * @param object the object
         * @return the object value
         */
        private String getObjectValue(Object object) {
            return object != null ? object.getClass().getSimpleName() : null;
        }
    }
}