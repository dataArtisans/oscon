package com.dataartisans.jobs;

import com.dataartisans.sources.TimestampSource;
import com.dataartisans.data.DataPoint;
import com.dataartisans.data.DataPointSerializationSchema;
import com.dataartisans.data.KeyedDataPoint;
import com.dataartisans.functions.AssignKeyFunction;
import com.dataartisans.functions.SawtoothFunction;
import com.dataartisans.functions.SineWaveFunction;
import com.dataartisans.functions.SquareWaveFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;

public class SensorSimulatorJob {

  public static void main(String[] args) throws Exception {

    // set up the execution environment
    final StreamExecutionEnvironment env =
      StreamExecutionEnvironment.getExecutionEnvironment();

    env.enableCheckpointing(1000);
    env.setParallelism(1);

    // Initial data - just timestamped messages
    DataStreamSource<DataPoint<Long>> timestampSource =
      env.addSource(new TimestampSource(100, 1), "test data");

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
    DataStream<KeyedDataPoint<Double>> sensorsStream =
      tempStream
        .union(pressureStream)
        .union(doorStream);

    // Write it to Kafka
    sensorsStream.addSink(new FlinkKafkaProducer09<>("localhost:9092", "sensors", new DataPointSerializationSchema()));

    // execute program
    env.execute("OSCON Data Generator");
  }

}
