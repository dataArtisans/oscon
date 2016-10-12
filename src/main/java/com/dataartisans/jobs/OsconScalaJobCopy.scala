package com.dataartisans.jobs

import java.lang.Double

import com.dataartisans.data.KeyedDataPoint.sum
import com.dataartisans.data.{ControlMessage, KeyedDataPoint}
import com.dataartisans.functions.{AmplifierFunction, MaxTimestampAssigner}
import com.dataartisans.sinks.InfluxDBSink
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object OsconScalaJobCopy {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)

    val stream = DataGenerator.stream(env)

    stream.addSink(new InfluxDBSink[KeyedDataPoint[Double]]("sensors"))

    val controlStream = env.socketTextStream("localhost", 9999)
      .map(ControlMessage.fromString(_))
      .assignTimestampsAndWatermarks(MaxTimestampAssigner[ControlMessage])

    val amplifiedstream = stream
      .keyBy(_.getKey)
      .connect(controlStream.keyBy(_.getKey))
      .flatMap(new AmplifierFunction)

    amplifiedstream
      .addSink(new InfluxDBSink[KeyedDataPoint[Double]]("amplifiedSensors"))

    amplifiedstream
      .keyBy(_.getKey)
      .timeWindow(Time.seconds(1))
      .reduce(sum _)
      .addSink(new InfluxDBSink[KeyedDataPoint[Double]]("summedSensors"))

    env.execute("Reactive Summit")
  }
}
