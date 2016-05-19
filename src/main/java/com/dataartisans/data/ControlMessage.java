package com.dataartisans.data;

public class ControlMessage {

  private String key;
  private double amplitude;

  public ControlMessage(){

  }

  public ControlMessage(String key, double amplitude){
    this.key = key;
    this.amplitude = amplitude;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public double getAmplitude() {
    return amplitude;
  }

  public void setAmplitude(double amplitude) {
    this.amplitude = amplitude;
  }

  public static ControlMessage fromString(String s){
    String[] parts = s.split(" ");
    if(parts.length == 2) {
      String key = parts[0];
      double amplitude = Double.valueOf(parts[1]);
      return new ControlMessage(key, amplitude);
    }
    else {
      return null;
    }
  }
}
