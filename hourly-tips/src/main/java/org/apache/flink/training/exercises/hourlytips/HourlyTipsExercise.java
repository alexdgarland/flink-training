/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.training.exercises.hourlytips;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator;
import org.apache.flink.training.exercises.common.utils.ExerciseBase;
import org.apache.flink.util.Collector;

/**
 * The "Hourly Tips" exercise of the Flink training in the docs.
 *
 * <p>The task of the exercise is to first calculate the total tips collected by each driver, hour by hour, and
 * then from that stream, find the highest tip total in each hour.
 *
 */
public class HourlyTipsExercise extends ExerciseBase {

	/**
	 * Main method.
	 *
	 * @throws Exception which occurs during job execution.
	 */
	public static void main(String[] args) throws Exception {

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(ExerciseBase.parallelism);

		// start the data generator
		DataStream<TaxiFare> fares = env.addSource(fareSourceOrTest(new TaxiFareGenerator()));

		WindowAssigner<Object, TimeWindow> windowAssigner = TumblingEventTimeWindows.of(Time.hours(1));

		SingleOutputStreamOperator<Tuple3<Long, Long, Float>> hourlyMax = fares
				.keyBy(fare -> fare.driverId)
				.window(windowAssigner)
				.aggregate(new DriverHourlyTipsAggregate())
				.windowAll(windowAssigner)
				.reduce(new MaxHourlyTipsReduce(), new MaxHourlyTipsWithWindowEnd())
		;

		printOrTest(hourlyMax);

		// execute the transformation pipeline
		env.execute("Hourly Tips (java)");
	}

	private static class DriverHourlyTipsAggregate implements AggregateFunction<
			TaxiFare, Tuple2<Long, Float>, Tuple2<Long, Float>
			> {

		@Override
		public Tuple2<Long, Float> createAccumulator() {
			return new Tuple2<>(null, 0F);
		}

		@Override
		public Tuple2<Long, Float> add(TaxiFare value, Tuple2<Long, Float> accumulator) {
			return new Tuple2<>(value.driverId, accumulator.f1 + value.tip);
		}

		@Override
		public Tuple2<Long, Float> getResult(Tuple2<Long, Float> accumulator) {
			return accumulator;
		}

		@Override
		public Tuple2<Long, Float> merge(Tuple2<Long, Float> a, Tuple2<Long, Float> b) {
			return new Tuple2<>(a.f0, a.f1 + b.f1);
		}
	}

	private static class MaxHourlyTipsReduce implements ReduceFunction<Tuple2<Long, Float>> {

		@Override
		public Tuple2<Long, Float> reduce(Tuple2<Long, Float> value1, Tuple2<Long, Float> value2)  {
			return value1.f1 > value2.f1 ? value1 : value2;
		}

	}

	private static class MaxHourlyTipsWithWindowEnd
			extends ProcessAllWindowFunction<Tuple2<Long, Float>, Tuple3<Long, Long, Float>, TimeWindow> {

		@Override
		public void process(
				Context context, Iterable<Tuple2<Long, Float>> elements, Collector<Tuple3<Long, Long, Float>> out
		) {
			Tuple2<Long, Float> max = elements.iterator().next();
			out.collect(Tuple3.of(context.window().getEnd(), max.f0, max.f1));
		}

	}

}
