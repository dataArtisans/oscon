package com.dataartisans.functions;

import com.dataartisans.data.DataPoint;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.checkpoint.CheckpointedAsynchronously;

public class SawtoothFunction extends RichMapFunction<DataPoint<Long>, DataPoint<Double>> implements CheckpointedAsynchronously<Integer> {

  final private int numSteps;

  // State!
  private int currentStep;

  public SawtoothFunction(int numSteps){
    this.numSteps = numSteps;
    this.currentStep = 0;
  }

  @Override
  public DataPoint<Double> map(DataPoint<Long> dataPoint) throws Exception {
    double phase = (double) currentStep / numSteps;
    currentStep = ++currentStep % numSteps;
    return dataPoint.withNewValue(phase);
  }

  @Override
  public Integer snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
    return currentStep;
  }

  @Override
  public void restoreState(Integer state) throws Exception {
    currentStep = state;
  }
}
