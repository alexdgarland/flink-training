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
public class HourlyTipsExerciseUsingReduce extends ExerciseBase {

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
				.map(DriverTips::new)
				.keyBy(t -> t.driverId)
				.window(windowAssigner)
				.reduce(DriverTips::add)
				.windowAll(windowAssigner)
				.reduce(DriverTips::max, new AddWindowEndToMaxHourlyTips())
		;

		printOrTest(hourlyMax);

		env.execute("Hourly Tips (java)");
	}

	private static class DriverTips {

		public final Long driverId;
		public final Float tips;

		private DriverTips(Long driverId, Float tips) {
			this.driverId = driverId;
			this.tips = tips;
		}

		public DriverTips(TaxiFare fare) {
			this(fare.driverId, fare.tip);
		}

		public DriverTips add(DriverTips other) { return new DriverTips(this.driverId, this.tips + other.tips); }

		public DriverTips max(DriverTips other) { return this.tips > other.tips ? this : other; }

		public Tuple3<Long, Long, Float> withWindowEnd(Long windowEnd) {
			return Tuple3.of(windowEnd, this.driverId, this.tips);
		}
	}

	private static class AddWindowEndToMaxHourlyTips extends
			ProcessAllWindowFunction<DriverTips, Tuple3<Long, Long, Float>, TimeWindow> {
		@Override
		public void process(Context context, Iterable<DriverTips> elements, Collector<Tuple3<Long, Long, Float>> out) {
			out.collect(elements.iterator().next().withWindowEnd(context.window().getEnd()));
		}
	}

}
