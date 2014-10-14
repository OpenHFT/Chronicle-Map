package net.openhft.chronicle.map;

import java.net.InetAddress;
import java.net.NetworkInterface;


final class UdpReplicationReplicatorConfigBean extends UdpReplicationReplicatorConfig {
    private final InetAddress address;
    private final int port;
    private final NetworkInterface networkInterface;
    private final ThrottlingConfig throttlingConfig;

    UdpReplicationReplicatorConfigBean(
            InetAddress address,
            int port,
            NetworkInterface networkInterface,
            ThrottlingConfig throttlingConfig) {
        if (address == null) {
            throw new NullPointerException("Null address");
        }
        this.address = address;
        this.port = port;

        this.networkInterface = networkInterface;
        if (throttlingConfig == null) {
            throw new NullPointerException("Null throttlingConfig");
        }
        this.throttlingConfig = throttlingConfig;
    }

    @Override
    public InetAddress address() {
        return address;
    }

    @Override
    public int port() {
        return port;
    }

    @Override
    public NetworkInterface networkInterface() {
        return networkInterface;
    }

    @Override
    public ThrottlingConfig throttlingConfig() {
        return throttlingConfig;
    }

    @Override
    public String toString() {
        return "UdpReplicationConfig{"
                + "address=" + address
                + ", port=" + port
                + ", networkInterface=" + networkInterface
                + ", throttlingConfig=" + throttlingConfig
                + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof UdpReplicationReplicatorConfig) {
            UdpReplicationReplicatorConfig that = (UdpReplicationReplicatorConfig) o;
            return (this.address.equals(that.address()))
                    && (this.port == that.port())
                    && (this.networkInterface.equals(that.networkInterface()))
                    && (this.throttlingConfig.equals(that.throttlingConfig()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= address.hashCode();
        h *= 1000003;
        h ^= port;
        h *= 1000003;
        h ^= networkInterface.hashCode();
        h *= 1000003;
        h ^= throttlingConfig.hashCode();
        return h;
    }
}
