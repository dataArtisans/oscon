package com.dataartisans.functions;

import com.dataartisans.data.DataPoint;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;

import java.util.Collections;
import java.util.List;

public class SawtoothFunction extends RichMapFunction<DataPoint<Long>, DataPoint<Double>> implements ListCheckpointed<Integer> {

  final private int numSteps;
  private Counter datapoints;

  // State!
  private int currentStep;

  public SawtoothFunction(int numSteps){
    this.numSteps = numSteps;
    this.currentStep = 0;
  }

  @Override
  public void open(Configuration config) {
    this.datapoints = getRuntimeContext()
            .getMetricGroup()
            .counter("datapoints");
  }

  @Override
  public DataPoint<Double> map(DataPoint<Long> dataPoint) throws Exception {
    double phase = (double) currentStep / numSteps;
    currentStep = ++currentStep % numSteps;
    this.datapoints.inc();
    return dataPoint.withNewValue(phase);
  }

  @Override
  public void restoreState(List<Integer> state) throws Exception {
    for (Integer s : state) {
      this.currentStep = s;
    }
  }

  @Override
  public List<Integer> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
    return Collections.singletonList(currentStep);
  }

}
