package com.dataartisans;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class OsconJobBackup {

  public static final int SLOWDOWN_FACTOR = 1;
  public static final int TIMESTAMP_PERIOD_MS = 1000 / 10;

  public static void main(String[] args) throws Exception {

    // set up the execution environment
    final StreamExecutionEnvironment env =
      StreamExecutionEnvironment.getExecutionEnvironment();

    env.enableCheckpointing(1000);
    env.setParallelism(1);
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1000, 1000));
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

    // Initial data - just timestamped messages
    DataStreamSource<DataPoint<Long>> timestampSource =
      env.addSource(new TimestampSource(TIMESTAMP_PERIOD_MS, SLOWDOWN_FACTOR));

    // Square Wave
    SingleOutputStreamOperator<DataPoint<Double>> squareWave =
      timestampSource
        .map(new SawtoothFunction(10));

    // Sine Wave
    SingleOutputStreamOperator<DataPoint<Double>> sineWave =
      squareWave
        .map(new SineWaveFunction());

    // Output raw square wave
    squareWave
      .addSink(new InfluxDBSink<>("squareWave"));

    // Output sine wave
    sineWave
      .addSink(new InfluxDBSink<>("sineWave"));

    // Output summed sine wave
    sineWave
      .timeWindowAll(Time.seconds(1))
      .sum("value")
      .addSink(new InfluxDBSink<>("summedSineWave"));

    // Output summed square wave
    squareWave
      .timeWindowAll(Time.seconds(1))
      .sum("value")
      .addSink(new InfluxDBSink<>("summedSquareWave"));

    // Generate some keyed data and output it
    timestampSource
      .flatMap(new KeyedDataGeneratorFunction())
      .keyBy("key")
      .timeWindow(Time.seconds(1))
      .sum("value")
      .addSink(new InfluxDBSink<>("keyedData"));

    // execute program
    env.execute("OSCON Example");
  }
}
