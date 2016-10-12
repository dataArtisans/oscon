package com.dataartisans.data;

public class KeyedDataPoint<T> extends DataPoint<T> {

  private String key;

  public KeyedDataPoint(){
    super();
    this.key = null;
  }

  public KeyedDataPoint(String key, long timeStampMs, T value) {
    super(timeStampMs, value);
    this.key = key;
  }

  @Override
  public String toString() {
    return getTimeStampMs() + "," + getKey() + "," + getValue();
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public <R> KeyedDataPoint<R> withNewValue(R newValue){
    return new KeyedDataPoint<>(this.getKey(), this.getTimeStampMs(), newValue);
  }

  public static KeyedDataPoint<Double> sum(KeyedDataPoint<Double> p1, KeyedDataPoint<Double> p2) {
    return p1.withNewValue(p1.getValue() + p2.getValue());
  }
}
