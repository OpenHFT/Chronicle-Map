package net.openhft.chronicle.map;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@javax.annotation.Generated("com.google.auto.value.processor.AutoValueProcessor")
final class TcpReplicationConfigBean extends TcpReplicationConfig {
  private final int serverPort;
  private final Set<InetSocketAddress> endpoints;
  private final int packetSize;
  private final ThrottlingConfig throttlingConfig;
  private final long heartBeatInterval;
  private final TimeUnit heartBeatIntervalUnit;

  TcpReplicationConfigBean(
          int serverPort,
          Set<InetSocketAddress> endpoints,
          int packetSize,
          ThrottlingConfig throttlingConfig,
          long heartBeatInterval,
          TimeUnit heartBeatIntervalUnit) {
    this.serverPort = serverPort;
    if (endpoints == null) {
      throw new NullPointerException("Null endpoints");
    }
    this.endpoints = endpoints;
    this.packetSize = packetSize;
    if (throttlingConfig == null) {
      throw new NullPointerException("Null throttlingConfig");
    }
    this.throttlingConfig = throttlingConfig;
    this.heartBeatInterval = heartBeatInterval;
    if (heartBeatIntervalUnit == null) {
      throw new NullPointerException("Null heartBeatIntervalUnit");
    }
    this.heartBeatIntervalUnit = heartBeatIntervalUnit;
  }

  @Override
  public int serverPort() {
    return serverPort;
  }

  @Override
  public Set<InetSocketAddress> endpoints() {
    return endpoints;
  }

  @Override
  public int packetSize() {
    return packetSize;
  }

  @Override
  public ThrottlingConfig throttlingConfig() {
    return throttlingConfig;
  }

  @Override
  long heartBeatInterval() {
    return heartBeatInterval;
  }

  @Override
  TimeUnit heartBeatIntervalUnit() {
    return heartBeatIntervalUnit;
  }

  @Override
  public String toString() {
    return "TcpReplicationConfig{"
        + "serverPort=" + serverPort
        + ", endpoints=" + endpoints
        + ", packetSize=" + packetSize
        + ", throttlingConfig=" + throttlingConfig
        + ", heartBeatInterval=" + heartBeatInterval
        + ", heartBeatIntervalUnit=" + heartBeatIntervalUnit
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof TcpReplicationConfig) {
      TcpReplicationConfig that = (TcpReplicationConfig) o;
      return (this.serverPort == that.serverPort())
          && (this.endpoints.equals(that.endpoints()))
          && (this.packetSize == that.packetSize())
          && (this.throttlingConfig.equals(that.throttlingConfig()))
          && (this.heartBeatInterval == that.heartBeatInterval())
          && (this.heartBeatIntervalUnit.equals(that.heartBeatIntervalUnit()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h = 1;
    h *= 1000003;
    h ^= serverPort;
    h *= 1000003;
    h ^= endpoints.hashCode();
    h *= 1000003;
    h ^= packetSize;
    h *= 1000003;
    h ^= throttlingConfig.hashCode();
    h *= 1000003;
    h ^= (heartBeatInterval >>> 32) ^ heartBeatInterval;
    h *= 1000003;
    h ^= heartBeatIntervalUnit.hashCode();
    return h;
  }
}
