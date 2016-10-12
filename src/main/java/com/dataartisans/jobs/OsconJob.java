package com.dataartisans.jobs;

import com.dataartisans.sinks.InfluxDBSink;
import com.dataartisans.data.KeyedDataPoint;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class OsconJob {

  public static void main(String[] args) throws Exception {

    // set up the execution environment
    final StreamExecutionEnvironment env =
      StreamExecutionEnvironment.getExecutionEnvironment();

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.enableCheckpointing(1000);

    // Simulate some sensor data
    DataStream<KeyedDataPoint<Double>> sensorStream = RawDataGenerator.stream(env);

    sensorStream
      .addSink(new InfluxDBSink<>("sensors"));

    sensorStream
      .keyBy("key")
      .timeWindow(Time.seconds(1))
      .sum("value")
      .addSink(new InfluxDBSink<>("summedSensors"));

    // execute program
    env.execute("OSCON Example");
  }
}
