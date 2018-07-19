package com.dataartisans.sources;

import com.dataartisans.data.DataPoint;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Collections;
import java.util.List;

public class TimestampSource extends RichSourceFunction<DataPoint<Long>> implements ListCheckpointed<Long> {
  private final int periodMs;
  private final int slowdownFactor;
  private volatile boolean running = true;

  // Checkpointed State
  private volatile long currentTimeMs = 0;

  public TimestampSource(int periodMs, int slowdownFactor){
    this.periodMs = periodMs;
    this.slowdownFactor = slowdownFactor;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    long now = System.currentTimeMillis();
    if(currentTimeMs == 0) {
      currentTimeMs = now - (now % 1000); // floor to second boundary
    }
  }

  @Override
  public void run(SourceContext<DataPoint<Long>> ctx) throws Exception {
    while (running) {
      synchronized (ctx.getCheckpointLock()) {
        ctx.collectWithTimestamp(new DataPoint<>(currentTimeMs, 0L), currentTimeMs);
        ctx.emitWatermark(new Watermark(currentTimeMs));
        currentTimeMs += periodMs;
      }
      timeSync();
    }
  }

  @Override
  public void cancel() {
    running = false;
  }

  @Override
  public List<Long> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
    return Collections.singletonList(currentTimeMs);
  }

  @Override
  public void restoreState(List<Long> state) throws Exception {
    for (Long s : state)
      currentTimeMs = s;
  }

  private void timeSync() throws InterruptedException {
    // Sync up with real time
    long realTimeDeltaMs = currentTimeMs - System.currentTimeMillis();
    long sleepTime = periodMs + realTimeDeltaMs + randomJitter();

    if(slowdownFactor != 1){
      sleepTime = periodMs * slowdownFactor;
    }

    if(sleepTime > 0) {
      Thread.sleep(sleepTime);
    }
  }

  private long randomJitter(){
    double sign = -1.0;
    if(Math.random() > 0.5){
      sign = 1.0;
    }
    return (long)(Math.random() * periodMs * sign);
  }
}
