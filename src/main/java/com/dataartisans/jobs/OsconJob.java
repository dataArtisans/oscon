package com.dataartisans.jobs;

import com.dataartisans.data.ControlMessage;
import com.dataartisans.functions.*;
import com.dataartisans.sinks.InfluxDBSink;
import com.dataartisans.sources.TimestampSource;
import com.dataartisans.data.DataPoint;
import com.dataartisans.data.KeyedDataPoint;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class OsconJob {

  public static void main(String[] args) throws Exception {

    // set up the execution environment
    final StreamExecutionEnvironment env =
      StreamExecutionEnvironment.getExecutionEnvironment();

    // Simulate some sensor data
    DataStream<KeyedDataPoint<Double>> sensorStream = generateSensorData(env);

    // Write this sensor stream out to InfluxDB
    sensorStream
      .addSink(new InfluxDBSink<>("sensors"));

    // Compute a windowed sum over this data and write that to InfluxDB as well.
    sensorStream
      .keyBy("key")
      .timeWindow(Time.seconds(1))
      .sum("value")
      .addSink(new InfluxDBSink<>("summedSensors"));

    // add a socket source
    KeyedStream<ControlMessage, Tuple> controlStream = env.socketTextStream("localhost", 9999)
      .map(msg -> ControlMessage.fromString(msg))
      .keyBy("key");

    // modulate sensor stream via control stream
    sensorStream
      .keyBy("key")
      .connect(controlStream)
      .flatMap(new AmplifierFunction())
      .addSink(new InfluxDBSink<>("amplifiedSensors"));

    // execute program
    env.execute("OSCON Example");
  }

  private static DataStream<KeyedDataPoint<Double>> generateSensorData(StreamExecutionEnvironment env) {

    // boiler plate for this demo
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1000, 1000));
    env.setParallelism(1);
    env.disableOperatorChaining();

    final int SLOWDOWN_FACTOR = 1;
    final int PERIOD_MS = 100;

    // Initial data - just timestamped messages
    DataStreamSource<DataPoint<Long>> timestampSource =
      env.addSource(new TimestampSource(PERIOD_MS, SLOWDOWN_FACTOR), "test data");

    // Transform into sawtooth pattern
    SingleOutputStreamOperator<DataPoint<Double>> sawtoothStream = timestampSource
      .map(new SawtoothFunction(10))
      .name("sawTooth");

    // Simulate temp sensor
    SingleOutputStreamOperator<KeyedDataPoint<Double>> tempStream = sawtoothStream
      .map(new AssignKeyFunction("temp"))
      .name("assignKey(temp)");

    // Make sine wave and use for pressure sensor
    SingleOutputStreamOperator<KeyedDataPoint<Double>> pressureStream = sawtoothStream
      .map(new SineWaveFunction())
      .name("sineWave")
      .map(new AssignKeyFunction("pressure"))
      .name("assignKey(pressure");

    // Make square wave and use for door sensor
    SingleOutputStreamOperator<KeyedDataPoint<Double>> doorStream = sawtoothStream
      .map(new SquareWaveFunction())
      .name("squareWave")
      .map(new AssignKeyFunction("door"))
      .name("assignKey(door)");

    // Combine all the streams into one and write it to Kafka
    DataStream<KeyedDataPoint<Double>> sensorStream =
      tempStream
        .union(pressureStream)
        .union(doorStream);

    return sensorStream;
  }

}
