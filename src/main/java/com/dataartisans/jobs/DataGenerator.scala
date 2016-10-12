package com.dataartisans.jobs

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object DataGenerator {
  def stream(env: StreamExecutionEnvironment) = new DataStream(RawDataGenerator.stream(env.getJavaEnv))
}
