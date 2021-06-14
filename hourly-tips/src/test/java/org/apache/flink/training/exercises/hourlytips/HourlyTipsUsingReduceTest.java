package org.apache.flink.training.exercises.hourlytips;

public class HourlyTipsUsingReduceTest extends AbstractHourlyTipsTest {

	@Override
	public Testable javaExercise() {
		return () -> HourlyTipsExerciseUsingReduce.main(new String[]{});
	}

}
