package com.dataartisans;

import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

class AmplifierFunction extends RichCoFlatMapFunction<KeyedDataPoint<Double>, ControlMessage, KeyedDataPoint<Double>> {

  ValueStateDescriptor<Double> stateDesc = new ValueStateDescriptor<>("amplitude", Double.class, 0.0);

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
  }

  @Override
  public void flatMap1(KeyedDataPoint<Double> dataPoint, Collector<KeyedDataPoint<Double>> collector) throws Exception {
    Double amplitude = getRuntimeContext().getState(stateDesc).value();
    collector.collect(dataPoint.withNewValue(dataPoint.getValue() * amplitude));
  }

  @Override
  public void flatMap2(ControlMessage msg, Collector<KeyedDataPoint<Double>> collector) throws Exception {
    getRuntimeContext().getState(stateDesc).update(msg.getAmplitude());
  }
}
