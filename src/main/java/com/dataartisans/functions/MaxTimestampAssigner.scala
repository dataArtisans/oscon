package com.dataartisans.functions

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

object MaxTimestampAssigner {
  def apply[T]() : AssignerWithPeriodicWatermarks[T] = { new MaxTimestampAssigner[T] }
}

class MaxTimestampAssigner[T] extends AssignerWithPeriodicWatermarks[T]{
  override def getCurrentWatermark: Watermark = new Watermark(Long.MaxValue)
  override def extractTimestamp(element: T, previousElementTimestamp: Long): Long = Long.MaxValue
}
