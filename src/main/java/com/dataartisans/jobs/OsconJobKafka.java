package com.dataartisans.jobs;

import com.dataartisans.functions.SensorDataWatermarkAssigner;
import com.dataartisans.data.DataPointSerializationSchema;
import com.dataartisans.data.KeyedDataPoint;
import com.dataartisans.sinks.InfluxDBSink;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;

import java.util.Properties;

public class OsconJobKafka {

  public static void main(String[] args) throws Exception {

    // set up the execution environment
    final StreamExecutionEnvironment env =
      StreamExecutionEnvironment.getExecutionEnvironment();

    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1000, 1000));
    env.enableCheckpointing(1000);
    env.setParallelism(1);
    env.disableOperatorChaining();

    boolean useEventTime = true;
    if(useEventTime){
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    }

    // Data Processor
    // Kafka consumer properties
    Properties kafkaProperties = new Properties();
    kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
    kafkaProperties.setProperty("group.id", "oscon-demo-group");

    // Create Kafka Consumer
    FlinkKafkaConsumer09<KeyedDataPoint<Double>> kafkaConsumer =
      new FlinkKafkaConsumer09<>("sensors", new DataPointSerializationSchema(), kafkaProperties);

    // Add it as a stream
    SingleOutputStreamOperator<KeyedDataPoint<Double>> sensorStream = env.addSource(kafkaConsumer);

    if(useEventTime){
      sensorStream = sensorStream.assignTimestampsAndWatermarks(new SensorDataWatermarkAssigner());
    }

    // Write this sensor stream out to InfluxDB
    sensorStream
      .addSink(new InfluxDBSink<>("sensors"));

    // Compute a windowed sum over this data and write that to InfluxDB as well.
    sensorStream
      .keyBy("key")
      .timeWindow(Time.seconds(1))
      .sum("value")
      .addSink(new InfluxDBSink<>("summedSensors"));

    // execute program
    env.execute("OSCON Example");
  }
}
