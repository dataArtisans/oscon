package com.dataartisans.data;

import com.dataartisans.data.KeyedDataPoint;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

import java.io.IOException;

public class DataPointSerializationSchema implements SerializationSchema<KeyedDataPoint<Double>>, DeserializationSchema<KeyedDataPoint<Double>> {
  @Override
  public byte[] serialize(KeyedDataPoint<Double> dataPoint) {
    String s =  dataPoint.getTimeStampMs() + "," + dataPoint.getKey() + "," + dataPoint.getValue();
    return s.getBytes();
  }

  @Override
  public KeyedDataPoint<Double> deserialize(byte[] bytes) throws IOException {
    String s = new String(bytes);
    String[] parts = s.split(",");
    long timestampMs = Long.valueOf(parts[0]);
    String key = parts[1];
    double value = Double.valueOf(parts[2]);
    return new KeyedDataPoint<>(key, timestampMs, value);
  }

  @Override
  public boolean isEndOfStream(KeyedDataPoint<Double> doubleKeyedDataPoint) {
    return false;
  }

  @Override
  public TypeInformation<KeyedDataPoint<Double>> getProducedType() {
    return TypeInformation.of(new TypeHint<KeyedDataPoint<Double>>(){});
  }
}
