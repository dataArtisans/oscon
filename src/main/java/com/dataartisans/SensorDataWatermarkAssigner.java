package com.dataartisans;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

class SensorDataWatermarkAssigner implements AssignerWithPunctuatedWatermarks<KeyedDataPoint<Double>> {
  @Override
  public Watermark checkAndGetNextWatermark(KeyedDataPoint<Double> dataPoint, long l) {
    return new Watermark(dataPoint.getTimeStampMs() - 1000);
  }

  @Override
  public long extractTimestamp(KeyedDataPoint<Double> doubleKeyedDataPoint, long l) {
    return doubleKeyedDataPoint.getTimeStampMs();
  }
}
