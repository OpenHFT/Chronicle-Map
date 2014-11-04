package net.openhft.chronicle.map.example;

import net.openhft.chronicle.hash.ChannelProvider;
import net.openhft.chronicle.hash.ChannelProviderBuilder;
import net.openhft.chronicle.hash.TcpReplicationConfig;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

import static org.junit.Assert.assertEquals;

/**
 * @author Rob Austin.
 */
public class ChannelExample {




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


    /**
     * Test ReplicatedChronicleMap where the Replicated is over a TCP Socket, but with 4 nodes
     *
     * @author Rob Austin.
     */
    public class ChannelReplicationTest {

        private ChronicleMap<CharSequence, CharSequence> favoriteComputerServer1;
        private ChronicleMap<CharSequence, CharSequence> favoriteComputerServer2;

        private ChronicleMap<CharSequence, CharSequence> favoriteColourServer2;
        private ChronicleMap<CharSequence, CharSequence> favoriteColourServer1;


        private ChannelProvider channelProviderServer1;
        private ChannelProvider channelProviderServer2;


        @Test
        public void test() throws IOException, InterruptedException {

    // server 1 with  identifier = 1
    {
        byte identifier = (byte) 1;

        TcpReplicationConfig tcpConfig = TcpReplicationConfig
                .of(8086, new InetSocketAddress("localhost", 8087))
                .heartBeatInterval(1, java.util.concurrent.TimeUnit.SECONDS);


        channelProviderServer1 = new ChannelProviderBuilder()
                .replicators(identifier, tcpConfig)
                .create();

        // this demotes favoriteColour
        short channel1 = (short) 1;

        favoriteColourServer1 = ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                .entries(1000)
                .channel(channelProviderServer1.createChannel(channel1)).create();

        favoriteColourServer1.put("peter", "green");

        // this demotes favoriteComputer
        short channel2 = (short) 2;

        favoriteComputerServer1 = ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                .entries(1000)
                .channel(channelProviderServer1.createChannel(channel2)).create();

        favoriteComputerServer1.put("peter", "dell");

    }

    // server 2 with  identifier = 2
    {

        byte identifier = (byte) 2;

        TcpReplicationConfig tcpConfig =
                TcpReplicationConfig.of(8087).heartBeatInterval(1, java.util.concurrent.TimeUnit.SECONDS);

        channelProviderServer2 = new ChannelProviderBuilder()
                .replicators(identifier, tcpConfig)
                .create();


        // this demotes favoriteColour
        short channel1 = (short) 1;

        favoriteColourServer2 = ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                .entries(1000)
                .channel(channelProviderServer2.createChannel(channel1)).create();


        favoriteColourServer2.put("rob", "blue");

        // this demotes favoriteComputer
        short channel2 = (short) 2;

        favoriteComputerServer2 = ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                .entries(1000)
                .channel(channelProviderServer2.createChannel(channel2)).create();

        favoriteComputerServer2.put("rob", "mac");
        favoriteComputerServer2.put("daniel", "mac");
    }


    // allow time for the recompilation to resolve
    for (int t = 0; t < 2500; t++) {
        if (favoriteComputerServer2.equals(favoriteComputerServer1) &&
                favoriteColourServer2.equals(favoriteColourServer1))
            break;
        Thread.sleep(1);
    }


    assertEquals(favoriteComputerServer1, favoriteComputerServer2);
    Assert.assertEquals(3, favoriteComputerServer2.size());


    assertEquals(favoriteColourServer1, favoriteColourServer2);
    Assert.assertEquals(2, favoriteColourServer1.size());


    favoriteColourServer1.close();
    favoriteComputerServer2.close();
    favoriteColourServer2.close();
    favoriteColourServer1.close();
}


    }

}



