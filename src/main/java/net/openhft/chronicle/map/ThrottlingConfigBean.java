package net.openhft.chronicle.map;

import java.util.concurrent.TimeUnit;

@javax.annotation.Generated("com.google.auto.value.processor.AutoValueProcessor")
final class ThrottlingConfigBean extends ThrottlingConfig {
  private final long throttle;
  private final TimeUnit throttlePerUnit;
  private final long bucketInterval;
  private final TimeUnit bucketIntervalUnit;

  ThrottlingConfigBean(
          long throttle,
          TimeUnit throttlePerUnit,
          long bucketInterval,
          TimeUnit bucketIntervalUnit) {
    this.throttle = throttle;
    if (throttlePerUnit == null) {
      throw new NullPointerException("Null throttlePerUnit");
    }
    this.throttlePerUnit = throttlePerUnit;
    this.bucketInterval = bucketInterval;
    if (bucketIntervalUnit == null) {
      throw new NullPointerException("Null bucketIntervalUnit");
    }
    this.bucketIntervalUnit = bucketIntervalUnit;
  }

  @Override
  long throttle() {
    return throttle;
  }

  @Override
  TimeUnit throttlePerUnit() {
    return throttlePerUnit;
  }

  @Override
  long bucketInterval() {
    return bucketInterval;
  }

  @Override
  TimeUnit bucketIntervalUnit() {
    return bucketIntervalUnit;
  }

  @Override
  public String toString() {
    return "ThrottlingConfig{"
        + "throttle=" + throttle
        + ", throttlePerUnit=" + throttlePerUnit
        + ", bucketInterval=" + bucketInterval
        + ", bucketIntervalUnit=" + bucketIntervalUnit
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof ThrottlingConfig) {
      ThrottlingConfig that = (ThrottlingConfig) o;
      return (this.throttle == that.throttle())
          && (this.throttlePerUnit.equals(that.throttlePerUnit()))
          && (this.bucketInterval == that.bucketInterval())
          && (this.bucketIntervalUnit.equals(that.bucketIntervalUnit()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h = 1;
    h *= 1000003;
    h ^= (throttle >>> 32) ^ throttle;
    h *= 1000003;
    h ^= throttlePerUnit.hashCode();
    h *= 1000003;
    h ^= (bucketInterval >>> 32) ^ bucketInterval;
    h *= 1000003;
    h ^= bucketIntervalUnit.hashCode();
    return h;
  }
}
